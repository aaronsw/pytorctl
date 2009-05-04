#!/usr/bin/python

"""

Support classes for statisics gathering in SQL Databases

DOCDOC

"""

import socket
import sys
import time
import datetime

import PathSupport, TorCtl
from TorUtil import *
from PathSupport import *
from TorUtil import meta_port, meta_host, control_port, control_host, control_pass
from TorCtl import EVENT_TYPE, EVENT_STATE, TorCtlError

import sqlalchemy
from sqlalchemy.orm import scoped_session, sessionmaker, eagerload, lazyload, eagerload_all
from sqlalchemy import create_engine, and_, or_, not_, func
from sqlalchemy.sql import func,select
from sqlalchemy.schema import ThreadLocalMetaData,MetaData
from elixir import *

# Nodes with a ratio below this value will be removed from consideration
# for higher-valued nodes
MIN_RATIO=0.5

NO_FPE=2**-50

#################### Model #######################

# In elixir, the session (DB connection) is a property of the model..
# There can only be one for all of the listeners below that use it
# See http://elixir.ematia.de/trac/wiki/Recipes/MultipleDatabases
OP=None
tc_metadata = MetaData()
tc_metadata.echo=False
tc_session = scoped_session(sessionmaker(autoflush=True))

def setup_db(db_uri, drop=False):
  tc_engine = create_engine(db_uri, echo=False)
  tc_metadata.bind = tc_engine
  tc_metadata.echo = False

  setup_all()
  if drop: drop_all()
  create_all()

class Router(Entity):
  using_options(order_by='-published', session=tc_session, metadata=tc_metadata)
  using_mapper_options(save_on_init=False)
  idhex = Field(CHAR(40), primary_key=True, index=True)
  orhash = Field(CHAR(27))
  published = Field(Time)
  nickname = Field(Text)

  os = Field(Text)
  rate_limited = Field(Boolean)
  guard = Field(Boolean)
  exit = Field(Boolean)
  stable = Field(Boolean)
  v2dir = Field(Boolean)
  v3dir = Field(Boolean)
  hsdir = Field(Boolean)

  bw = Field(Integer)
  version = Field(Integer)
  # FIXME: is mutable=False what we want? Do we care?
  router = Field(PickleType(mutable=False)) 
  circuits = ManyToMany('Circuit')
  streams = ManyToMany('Stream')
  detached_streams = ManyToMany('Stream')
  bw_history = OneToMany('BwHistory')
  stats = OneToOne('RouterStats', inverse="router")

  def from_router(self, router):
    self.published = router.published
    self.bw = router.bw
    self.idhex = router.idhex
    self.orhash = router.orhash
    self.nickname = router.nickname
    self.os = router.os
    self.rate_limited = router.rate_limited
    self.guard = "Guard" in router.flags
    self.exit = "Exit" in router.flags
    self.stable = "Stable" in router.flags
    self.v2dir = "V2Dir" in router.flags
    self.v3dir = "V3Dir" in router.flags
    self.hsdir = "HSDir" in router.flags
    self.version = router.version.version
    self.router = router #pickle.dumps(router)
    return self

class BwHistory(Entity):
  using_options(session=tc_session, metadata=tc_metadata)
  using_mapper_options(save_on_init=False)
  router = ManyToOne('Router')
  bw = Field(Integer)
  rank = Field(Integer)
  pub_time = Field(Time)

class Circuit(Entity):
  using_options(order_by='-launch_time', session=tc_session, metadata=tc_metadata)
  using_mapper_options(save_on_init=False)
  routers = ManyToMany('Router')
  streams = OneToMany('Stream', inverse='circuit')
  detached_streams = ManyToMany('Stream', inverse='detached_circuits')
  extensions = OneToMany('Extension', inverse='circ')
  circ_id = Field(Integer, index=True)
  launch_time = Field(Float)
  last_extend = Field(Float)

class FailedCircuit(Circuit):
  using_mapper_options(save_on_init=False)
  using_options(session=tc_session, metadata=tc_metadata)
  #failed_extend = ManyToOne('Extension', inverse='circ')
  fail_reason = Field(Text)
  fail_time = Field(Float)

class BuiltCircuit(Circuit):
  using_options(session=tc_session, metadata=tc_metadata)
  using_mapper_options(save_on_init=False)
  built_time = Field(Float)
  tot_delta = Field(Float)

class DestroyedCircuit(Circuit):
  using_options(session=tc_session, metadata=tc_metadata)
  using_mapper_options(save_on_init=False)
  destroy_reason = Field(Text)
  destroy_time = Field(Float)

class ClosedCircuit(BuiltCircuit):
  using_options(session=tc_session, metadata=tc_metadata)
  using_mapper_options(save_on_init=False)
  closed_time = Field(Float)

class Extension(Entity):
  using_mapper_options(save_on_init=False)
  using_options(order_by='-time', session=tc_session, metadata=tc_metadata)
  circ = ManyToOne('Circuit', inverse='extensions')
  from_node = ManyToOne('Router')
  to_node = ManyToOne('Router')
  hop = Field(Integer)
  time = Field(Float)
  delta = Field(Float)

class FailedExtension(Extension):
  using_options(session=tc_session, metadata=tc_metadata)
  #failed_circ = ManyToOne('FailedCircuit', inverse='failed_extend')
  using_mapper_options(save_on_init=False)
  reason = Field(Text)

class Stream(Entity):
  using_options(session=tc_session, metadata=tc_metadata)
  using_options(order_by='-start_time')
  using_mapper_options(save_on_init=False)
  tgt_host = Field(Text)
  tgt_port = Field(Integer)
  circuit = ManyToOne('Circuit', inverse='streams')
  detached_circuits = ManyToMany('Circuit', inverse='detatched_streams')
  ignored = Field(Boolean) # Directory streams
  strm_id = Field(Integer, index=True)
  start_time = Field(Float)
  tot_read_bytes = Field(Integer)
  tot_write_bytes = Field(Integer)
  init_status = Field(Text)
  close_reason = Field(Text) # Shared by Failed and Closed. Unused here.

class FailedStream(Stream):
  using_options(session=tc_session, metadata=tc_metadata)
  using_mapper_options(save_on_init=False)
  fail_reason = Field(Text)
  fail_time = Field(Float)

class ClosedStream(Stream):
  using_options(session=tc_session, metadata=tc_metadata)
  using_mapper_options(save_on_init=False)
  end_time = Field(Float)
  read_bandwidth = Field(Float)
  write_bandwidth = Field(Float)

class RouterStats(Entity):
  using_options(session=tc_session, metadata=tc_metadata)
  using_mapper_options(save_on_init=False)
  router = ManyToOne('Router', inverse="stats")
   
  # Unused
  circ_used = Field(Integer) # Extended up to this node
  circ_fail = Field(Integer) # Includes timeouts of priors

  # Easily derived from BwHistory
  min_rank = Field(Integer)
  avg_rank = Field(Integer)
  max_rank = Field(Integer)
  avg_bw = Field(Float)

  percentile = Field(Float)

  # These can be derived with a single query over 
  # FailedExtension and Extension
  circ_fail_to = Field(Integer) 
  circ_fail_from = Field(Integer)
  circ_try_to = Field(Integer)
  circ_try_from = Field(Integer)

  circ_from_rate = Field(Float)
  circ_to_rate = Field(Float)
  circ_bi_rate = Field(Float)

  circ_to_ratio = Field(Float)
  circ_from_ratio = Field(Float)
  circ_bi_ratio = Field(Float)

  avg_first_ext = Field(Float)
  ext_ratio = Field(Float)
  
  sbw = Field(Float)
  sbw_ratio = Field(Float)
  filt_sbw = Field(Float)
  filt_sbw_ratio = Field(Float)

  def _compute_stats_relation(stats_clause):
    for rs in RouterStats.query.\
                   filter(stats_clause).\
                   options(eagerload_all('router.circuits.extensions')).\
                   all():
      rs.circ_fail_to = 0
      rs.circ_try_to = 0
      rs.circ_fail_from = 0
      rs.circ_try_from = 0
      tot_extend_time = 0
      tot_extends = 0
      for c in rs.router.circuits: 
        for e in c.extensions: 
          if e.to_node == r:
            rs.circ_try_to += 1
            if isinstance(e, FailedExtension):
              rs.circ_fail_to += 1
            elif e.hop == 0:
              tot_extend_time += e.delta
              tot_extends += 1
          elif e.from_node == r:
            rs.circ_try_from += 1
            if isinstance(e, FailedExtension):
              rs.circ_fail_from += 1
            
        if isinstance(c, FailedCircuit):
          pass # TODO: Also count timeouts against earlier nodes?
        elif isinstance(c, DestroyedCircuit):
          pass # TODO: Count these somehow..

      if tot_extends > 0: rs.avg_first_ext = (1.0*tot_extend_time)/tot_extends
      else: rs.avg_first_ext = 0
      if rs.circ_try_from > 0:
        rs.circ_from_rate = (1.0*rs.circ_fail_from/rs.circ_try_from)
      if rs.circ_try_to > 0:
        rs.circ_to_rate = (1.0*rs.circ_fail_to/rs.circ_try_to)
      if rs.circ_try_to+rs.circ_try_from > 0:
        rs.circ_bi_rate = (1.0*rs.circ_fail_to+rs.circ_fail_from)/(rs.circ_try_to+rs.circ_try_from)

      tc_session.add(rs)
    tc_session.commit()
  _compute_stats_relation = Callable(_compute_stats_relation)

  def _compute_stats_query(stats_clause):
    tc_session.clear()
    # http://www.sqlalchemy.org/docs/04/sqlexpression.html#sql_update
    to_s = select([func.count(Extension.id)], 
        and_(stats_clause, Extension.table.c.to_node_idhex
             == RouterStats.table.c.router_idhex,
             Extension.table.c.row_type=='extension')).as_scalar()
    from_s = select([func.count(Extension.id)], 
        and_(stats_clause, Extension.table.c.from_node_idhex
             == RouterStats.table.c.router_idhex,
             Extension.table.c.row_type=='extension')).as_scalar()
    f_to_s = select([func.count(FailedExtension.id)], 
        and_(stats_clause, FailedExtension.table.c.to_node_idhex
             == RouterStats.table.c.router_idhex,
             Extension.table.c.row_type=='faledextension')).as_scalar()
    f_from_s = select([func.count(FailedExtension.id)], 
        and_(stats_clause, FailedExtension.table.c.from_node_idhex
                       == RouterStats.table.c.router_idhex,
             Extension.table.c.row_type=='faledextension')).as_scalar()
    avg_ext = select([func.avg(Extension.delta)], 
        and_(stats_clause,
             Extension.table.c.to_node_idhex==RouterStats.table.c.router_idhex,
             Extension.table.c.hop==0, 
             Extension.table.c.row_type=='extension')).as_scalar()

    RouterStats.table.update(stats_clause, values=
      {RouterStats.table.c.circ_try_to:to_s,
       RouterStats.table.c.circ_try_from:from_s,
       RouterStats.table.c.circ_fail_to:f_to_s,
       RouterStats.table.c.circ_fail_from:f_from_s,
       RouterStats.table.c.avg_first_ext:avg_ext}).execute()

    RouterStats.table.update(stats_clause, values=
      {RouterStats.table.c.circ_from_rate:
         RouterStats.table.c.circ_fail_from/RouterStats.table.c.circ_try_from,
       RouterStats.table.c.circ_to_rate:
          RouterStats.table.c.circ_fail_to/RouterStats.table.c.circ_try_to,
       RouterStats.table.c.circ_bi_rate:
         (RouterStats.table.c.circ_fail_to+RouterStats.table.c.circ_fail_from)
                          /
      (RouterStats.table.c.circ_try_to+RouterStats.table.c.circ_try_from)}).execute()


    # TODO: Give the streams relation table a sane name and reduce this too
    for rs in RouterStats.query.filter(stats_clause).\
                        options(eagerload('router'), 
                                eagerload('router.streams')).all():
      tot_bw = 0.0
      s_cnt = 0
      for s in rs.router.streams:
        if isinstance(s, ClosedStream):
          tot_bw += s.read_bandwidth
          s_cnt += 1
      if s_cnt > 0: rs.sbw = tot_bw/s_cnt
      else: rs.sbw = None
      tc_session.add(rs)
    tc_session.commit()
  _compute_stats_query = Callable(_compute_stats_query)

  def _compute_stats(stats_clause):
    RouterStats._compute_stats_query(stats_clause)
    #RouterStats._compute_stats_relation(stats_clause)
  _compute_stats = Callable(_compute_stats)

  def _compute_ranks():
    tc_session.clear()
    min_r = select([func.min(BwHistory.rank)], 
        BwHistory.table.c.router_idhex
            == RouterStats.table.c.router_idhex).as_scalar()
    avg_r = select([func.avg(BwHistory.rank)], 
        BwHistory.table.c.router_idhex
            == RouterStats.table.c.router_idhex).as_scalar()
    max_r = select([func.max(BwHistory.rank)], 
        BwHistory.table.c.router_idhex
            == RouterStats.table.c.router_idhex).as_scalar()
    avg_bw = select([func.avg(BwHistory.bw)], 
        BwHistory.table.c.router_idhex
            == RouterStats.table.c.router_idhex).as_scalar()

    RouterStats.table.update(values=
       {RouterStats.table.c.min_rank:min_r,
        RouterStats.table.c.avg_rank:avg_r,
        RouterStats.table.c.max_rank:max_r,
        RouterStats.table.c.avg_bw:avg_bw}).execute()

    #min_avg_rank = select([func.min(RouterStats.avg_rank)]).as_scalar()
    max_avg_rank = select([func.max(RouterStats.avg_rank)]).as_scalar()

    RouterStats.table.update(values=
       {RouterStats.table.c.percentile:
            (100.0*RouterStats.table.c.avg_rank)/max_avg_rank}).execute()
    tc_session.commit()
  _compute_ranks = Callable(_compute_ranks)

  def _compute_ratios(stats_clause):
    tc_session.clear()
    avg_from_rate = select([func.avg(RouterStats.circ_from_rate)],
                           stats_clause).as_scalar()
    avg_to_rate = select([func.avg(RouterStats.circ_to_rate)],
                           stats_clause).as_scalar()
    avg_bi_rate = select([func.avg(RouterStats.circ_bi_rate)],
                           stats_clause).as_scalar()
    avg_ext = select([func.avg(RouterStats.avg_first_ext)],
                           stats_clause).as_scalar()
    avg_sbw = select([func.avg(RouterStats.sbw)],
                           stats_clause).as_scalar()

    RouterStats.table.update(stats_clause, values=
       {RouterStats.table.c.circ_from_ratio:
         (1-RouterStats.table.c.circ_from_rate)/(1-avg_from_rate),
        RouterStats.table.c.circ_to_ratio:
         (1-RouterStats.table.c.circ_to_rate)/(1-avg_to_rate),
        RouterStats.table.c.circ_bi_ratio:
         (1-RouterStats.table.c.circ_bi_rate)/(1-avg_bi_rate),
        RouterStats.table.c.ext_ratio:
         (RouterStats.table.c.avg_first_ext)/(avg_ext),
        RouterStats.table.c.sbw_ratio:
         (RouterStats.table.c.sbw)/(avg_sbw)}).execute()
    tc_session.commit()
  _compute_ratios = Callable(_compute_ratios)

  def _compute_filtered_query(min_ratio): # broken.. don't use.
    badrouters = RouterStats.query.filter(
       RouterStats.sbw_ratio < min_ratio).column(RouterStats.router).all()
  
    for r in Router.query.all():
      rs = r.stats
      # XXX: This is totally wrong:
      strmq = Router.query.filter_by(idhex=r.idhex).add_column(Router.streams).filter_by(row_type='closedstream')
      for br in badrouters:
        if br != r:
          strmq = strmq.filter(not_(ClosedStream.circuit.routers.contains(r)))
      rs.filt_sbw = strmq.avg(ClosedStream.read_bandwidth)
    avg_sbw = RouterStats.query.filter('1=1').avg(RouterStats.filt_sbw)
    for rs in RouterStats.query.all():
      rs.filt_sbw_ratio = rs.filt_sbw/avg_sbw
    tc_session.commit()
  _compute_filtered_query = Callable(_compute_filtered_query)

  def _compute_filtered_relational(min_ratio, stats_clause, filter_clause):
    badrouters = RouterStats.query.filter(stats_clause).filter(filter_clause).\
                   filter(RouterStats.sbw_ratio < min_ratio).all()

    # TODO: Turn this into a single query....
    for rs in RouterStats.query.filter(stats_clause).\
          options(eagerload_all('router.streams.circuit.routers')).all():
      tot_sbw = 0
      sbw_cnt = 0
      for s in rs.router.streams:
        if isinstance(s, ClosedStream):
          skip = False
          for br in badrouters:
            if br != rs:
              if br.router in s.circuit.routers:
                skip = True
          if not skip:
            tot_sbw += s.read_bandwidth   
            sbw_cnt += 1
      if sbw_cnt: rs.filt_sbw = tot_sbw/sbw_cnt
      else: rs.filt_sbw = None
      tc_session.add(rs)
    if sqlalchemy.__version__ < "0.5.0":
      avg_sbw = RouterStats.query.filter(stats_clause).avg(RouterStats.filt_sbw)
    else:
      avg_sbw = tc_session.query(func.avg(RouterStats.filt_sbw)).filter(stats_clause).scalar()
    for rs in RouterStats.query.filter(stats_clause).all():
      if type(rs.filt_sbw) == float and avg_sbw:
        rs.filt_sbw_ratio = rs.filt_sbw/avg_sbw
      else:
        rs.filt_sbw_ratio = None
      tc_session.add(rs)
    tc_session.commit()
  _compute_filtered_relational = Callable(_compute_filtered_relational)

  def _compute_filtered_ratios(min_ratio, stats_clause, filter_clause):
    RouterStats._compute_filtered_relational(min_ratio, stats_clause, 
                                             filter_clause)
    #RouterStats._compute_filtered_query(filter,min_ratio)
  _compute_filtered_ratios = Callable(_compute_filtered_ratios)

  def reset():
    RouterStats.table.drop()
    RouterStats.table.create()
    for r in Router.query.all(): # Is this needed?
      rs = RouterStats()
      rs.router = r
      r.stats = rs
      tc_session.add(r)
    tc_session.clear()
    tc_session.commit()
  reset = Callable(reset)

  def compute(pct_low=0, pct_high=100, stat_clause=None, filter_clause=None):
    pct_clause = and_(RouterStats.percentile >= pct_low, 
                         RouterStats.percentile < pct_high)
    if stat_clause:
      stat_clause = and_(pct_clause, stat_clause)
    else:
      stat_clause = pct_clause
     
    RouterStats.reset()
    RouterStats._compute_ranks() # No filters. Ranks are independent
    RouterStats._compute_stats(stat_clause)
    RouterStats._compute_ratios(stat_clause)
    RouterStats._compute_filtered_ratios(MIN_RATIO, stat_clause, filter_clause)
    tc_session.commit()
  compute = Callable(compute)  

  def write_stats(f, pct_low=0, pct_high=100, order_by=None, recompute=False, stat_clause=None, filter_clause=None):

    if not order_by:
      order_by=RouterStats.avg_first_ext

    if recompute:
      RouterStats.compute(pct_low, pct_high, stat_clause, filter_clause)

    pct_clause = and_(RouterStats.percentile >= pct_low, 
                         RouterStats.percentile < pct_high)

    # This is Fail City and sqlalchemy is running for mayor.
    if sqlalchemy.__version__ < "0.5.0":
      circ_from_rate = RouterStats.query.filter(pct_clause).filter(stat_clause).avg(RouterStats.circ_from_rate)
      circ_to_rate = RouterStats.query.filter(pct_clause).filter(stat_clause).avg(RouterStats.circ_to_rate)
      circ_bi_rate = RouterStats.query.filter(pct_clause).filter(stat_clause).avg(RouterStats.circ_bi_rate)

      avg_first_ext = RouterStats.query.filter(pct_clause).filter(stat_clause).avg(RouterStats.avg_first_ext)
      sbw = RouterStats.query.filter(pct_clause).filter(stat_clause).avg(RouterStats.sbw)
      filt_sbw = RouterStats.query.filter(pct_clause).filter(stat_clause).avg(RouterStats.filt_sbw)
      percentile = RouterStats.query.filter(pct_clause).filter(stat_clause).avg(RouterStats.percentile)
    else:
      circ_from_rate = tc_session.query(func.avg(RouterStats.circ_from_rate)).filter(pct_clause).filter(stat_clause).scalar()
      circ_to_rate = tc_session.query(func.avg(RouterStats.circ_to_rate)).filter(pct_clause).filter(stat_clause).scalar()
      circ_bi_rate = tc_session.query(func.avg(RouterStats.circ_bi_rate)).filter(pct_clause).filter(stat_clause).scalar()
      
      avg_first_ext = tc_session.query(func.avg(RouterStats.avg_first_ext)).filter(pct_clause).filter(stat_clause).scalar()
      sbw = tc_session.query(func.avg(RouterStats.sbw)).filter(pct_clause).filter(stat_clause).scalar()
      filt_sbw = tc_session.query(func.avg(RouterStats.filt_sbw)).filter(pct_clause).filter(stat_clause).scalar()
      percentile = tc_session.query(func.avg(RouterStats.percentile)).filter(pct_clause).filter(stat_clause).scalar()

    def cvt(a,b,c=1):
      if type(a) == float: return round(a/c,b)
      elif type(a) == type(None): return "None"
      else: return type(a)

    sql_key = """SQLSupport Statistics:
    CFR=Circ From Rate         CTR=Circ To Rate     CBR=Circ To/From Rate
    CFE=Avg 1st Ext time (s)   SBW=Avg Stream BW    FBW=Filtered stream bw
    RF=Circ From Ratio         RT=Circ To Ratio     RB=Circ To/From Ratio
    RE=1st Ext Ratio           RS=Stream BW Ratio   RF=Filt Stream Ratio
    PR=Percentile Rank\n\n"""
 
    f.write(sql_key)
    f.write("Average Statistics:\n")
    f.write("   CFR="+str(cvt(circ_from_rate,2))+" ")
    f.write(" CTR="+str(cvt(circ_to_rate,2))+" ")
    f.write(" CBR="+str(cvt(circ_bi_rate,2))+" ")
    f.write(" CFE="+str(cvt(avg_first_ext,2))+" ")
    f.write(" SBW="+str(cvt(sbw,2,1024))+" ")
    f.write(" FBW="+str(cvt(filt_sbw,2,1024))+" ")
    f.write(" PR="+str(cvt(percentile,2))+"\n\n\n")

    for s in RouterStats.query.filter(pct_clause).filter(stat_clause).\
           order_by(order_by).all():
      f.write(s.router.idhex+" ("+s.router.nickname+")\n")
      f.write("   CFR="+str(cvt(s.circ_from_rate,2))+" ")
      f.write(" CTR="+str(cvt(s.circ_to_rate,2))+" ")
      f.write(" CBR="+str(cvt(s.circ_bi_rate,2))+" ")
      f.write(" CFE="+str(cvt(s.avg_first_ext,2))+" ")
      f.write(" SBW="+str(cvt(s.sbw,2,1024))+" ")
      f.write(" FBW="+str(cvt(s.filt_sbw,2,1024))+" ")
      f.write(" PR="+str(cvt(s.percentile,1))+"\n")
      f.write("   RF="+str(cvt(s.circ_from_ratio,2))+" ")
      f.write(" RT="+str(cvt(s.circ_to_ratio,2))+" ")
      f.write(" RB="+str(cvt(s.circ_bi_ratio,2))+" ")
      f.write(" RE="+str(cvt(s.ext_ratio,2))+" ")
      f.write(" RS="+str(cvt(s.sbw_ratio,2))+" ")
      f.write(" RF="+str(cvt(s.filt_sbw_ratio,2))+"\n\n")

    f.flush()
  write_stats = Callable(write_stats)  
    

##################### End Model ####################

#################### Model Support ################
def reset_all():
  # Need to keep routers around.. 
  for r in Router.query.all():
    r.bw_history = [] # XXX: Is this sufficient/correct/necessary?
    r.circuits = []
    r.streams = []
    r.stats = None
    tc_session.add(r)

  BwHistory.table.drop() # Will drop subclasses
  Extension.table.drop()
  Stream.table.drop() 
  Circuit.table.drop()
  RouterStats.table.drop()

  RouterStats.table.create()
  BwHistory.table.create() 
  Extension.table.create()
  Stream.table.create() 
  Circuit.table.create()

  tc_session.commit()

##################### End Model Support ####################

class ConsensusTrackerListener(TorCtl.DualEventListener):
  def __init__(self):
    TorCtl.DualEventListener.__init__(self)
    self.last_desc_at = time.time()-10.0
    self.consensus = None

  # TODO: What about non-running routers and uptime information?
  def _update_rank_history(self, idlist):
    for idhex in idlist:
      if idhex not in self.consensus.routers: continue
      rc = self.consensus.routers[idhex]
      if rc.down: continue
      r = Router.query.options(eagerload('bw_history')).filter_by(
                                  idhex=idhex).one()
      bwh = BwHistory(router=r, rank=rc.list_rank, bw=rc.bw, 
                      pub_time=r.published)
      r.bw_history.append(bwh)
      tc_session.add(bwh)
      tc_session.add(r)
    tc_session.commit()
 
  def _update_db(self, idlist):
    for idhex in idlist:
      if idhex in self.consensus.routers:
        rc = self.consensus.routers[idhex]
        r = Router.query.filter_by(idhex=rc.idhex).first()
        
        if r and r.orhash == rc.orhash:
          # We already have it stored. (Possible spurious NEWDESC)
          continue

        if not r: r = Router()
 
        r.from_router(rc)
        tc_session.add(r)
    tc_session.commit()

  def update_consensus(self):
    self.consensus = self.parent_handler.current_consensus()
    self._update_db(self.consensus.ns_map.iterkeys())

  def set_parent(self, parent_handler):
    if not isinstance(parent_handler, TorCtl.ConsensusTracker):
      raise TorCtlError("ConsensusTrackerListener can only be attached to ConsensusTracker instances")
    TorCtl.DualEventListener.set_parent(self, parent_handler)

  def heartbeat_event(self, e):
    # This sketchiness is to ensure we have an accurate history
    # of each router's rank+bandwidth for the entire duration of the run..
    if e.state == EVENT_STATE.PRELISTEN:
      if not self.consensus: 
        global OP
        OP = Router.query.filter_by(
                 idhex="0000000000000000000000000000000000000000").first()
        if not OP:
          OP = Router(idhex="0000000000000000000000000000000000000000", 
                    orhash="000000000000000000000000000", 
                    nickname="!!TorClient", 
                    published=datetime.datetime.utcnow())
          tc_session.add(OP)
          tc_session.commit()
        self.update_consensus()
      # So ghetto
      if e.arrived_at - self.last_desc_at > 30.0:
        plog("INFO", "Newdesc timer is up. Assuming we have full consensus now")
        self.last_desc_at = 0x7fffffff
        self._update_rank_history(self.consensus.ns_map.iterkeys())

  def new_consensus_event(self, n):
    if n.state == EVENT_STATE.POSTLISTEN:
      self.last_desc_at = n.arrived_at
      self.update_consensus()

  def new_desc_event(self, d): 
    if d.state == EVENT_STATE.POSTLISTEN:
      self.last_desc_at = d.arrived_at
      self.consensus = self.parent_handler.current_consensus()
      self._update_db(d.idlist)

class CircuitListener(TorCtl.PreEventListener):
  def set_parent(self, parent_handler):
    if not filter(lambda f: f.__class__ == ConsensusTrackerListener, 
                  parent_handler.post_listeners):
       raise TorCtlError("CircuitListener needs a ConsensusTrackerListener")
    TorCtl.PreEventListener.set_parent(self, parent_handler)
    # TODO: This is really lame. We only know the extendee of a circuit
    # if we have built the path ourselves. Otherwise, Tor keeps it a
    # secret from us. This prevents us from properly tracking failures
    # for normal Tor usage.
    if isinstance(parent_handler, PathSupport.PathBuilder):
      self.track_parent = True
    else:
      self.track_parent = False

  def circ_status_event(self, c):
    if self.track_parent and c.circ_id not in self.parent_handler.circuits:
      return # Ignore circuits that aren't ours
    # TODO: Hrmm, consider making this sane in TorCtl.
    if c.reason: lreason = c.reason
    else: lreason = "NONE"
    if c.remote_reason: rreason = c.remote_reason
    else: rreason = "NONE"
    reason = c.event_name+":"+c.status+":"+lreason+":"+rreason

    output = [str(c.arrived_at), str(time.time()-c.arrived_at), c.event_name, str(c.circ_id), c.status]
    if c.path: output.append(",".join(c.path))
    if c.reason: output.append("REASON=" + c.reason)
    if c.remote_reason: output.append("REMOTE_REASON=" + c.remote_reason)
    plog("DEBUG", " ".join(output))
  
    if c.status == "LAUNCHED":
      circ = Circuit(circ_id=c.circ_id,launch_time=c.arrived_at,
                     last_extend=c.arrived_at)
      if self.track_parent:
        for r in self.parent_handler.circuits[c.circ_id].path:
          rq = Router.query.options(eagerload('circuits')).filter_by(
                                idhex=r.idhex).one()
          circ.routers.append(rq) 
          #rq.circuits.append(circ) # done automagically?
          #tc_session.add(rq)
      tc_session.add(circ)
      tc_session.commit()
    elif c.status == "EXTENDED":
      circ = Circuit.query.options(eagerload('extensions')).filter_by(
                       circ_id = c.circ_id).first()
      if not circ: return # Skip circuits from before we came online

      e = Extension(circ=circ, hop=len(c.path)-1, time=c.arrived_at)

      if len(c.path) == 1:
        e.from_node = OP
      else:
        r_ext = c.path[-2]
        if r_ext[0] != '$': r_ext = self.parent_handler.name_to_key[r_ext]
        e.from_node = Router.query.filter_by(idhex=r_ext[1:]).one()

      r_ext = c.path[-1]
      if r_ext[0] != '$': r_ext = self.parent_handler.name_to_key[r_ext]

      e.to_node = Router.query.filter_by(idhex=r_ext[1:]).one()
      if not self.track_parent:
        # FIXME: Eager load here?
        circ.routers.append(e.to_node)
        e.to_node.circuits.append(circ)
        tc_session.add(e.to_node)
 
      e.delta = c.arrived_at - circ.last_extend
      circ.last_extend = c.arrived_at
      circ.extensions.append(e)
      tc_session.add(e)
      tc_session.add(circ)
      tc_session.commit()
    elif c.status == "FAILED":
      circ = Circuit.query.filter_by(circ_id = c.circ_id).first()
      if not circ: return # Skip circuits from before we came online
        
      circ.expunge()
      if isinstance(circ, BuiltCircuit):
        # Convert to destroyed circuit
        Circuit.table.update(Circuit.id ==
                  circ.id).execute(row_type='destroyedcircuit')
        circ = DestroyedCircuit.query.filter_by(id=circ.id).one()
        circ.destroy_reason = reason
        circ.destroy_time = c.arrived_at
      else:
        # Convert to failed circuit
        Circuit.table.update(Circuit.id ==
                  circ.id).execute(row_type='failedcircuit')
        circ = FailedCircuit.query.options(
                  eagerload('extensions')).filter_by(id=circ.id).one()
        circ.fail_reason = reason
        circ.fail_time = c.arrived_at
        e = FailedExtension(circ=circ, hop=len(c.path), time=c.arrived_at)

        if len(c.path) == 0:
          e.from_node = OP
        else:
          r_ext = c.path[-1]
          if r_ext[0] != '$': r_ext = self.parent_handler.name_to_key[r_ext]
 
          e.from_node = Router.query.filter_by(idhex=r_ext[1:]).one()

        if self.track_parent:
          r=self.parent_handler.circuits[c.circ_id].path[len(c.path)]
          e.to_node = Router.query.filter_by(idhex=r.idhex).one()
        else:
          e.to_node = None # We have no idea..

        e.delta = c.arrived_at - circ.last_extend
        e.reason = reason
        circ.extensions.append(e)
        circ.fail_time = c.arrived_at
        tc_session.add(e)

      tc_session.add(circ)
      tc_session.commit()
    elif c.status == "BUILT":
      circ = Circuit.query.filter_by(
                     circ_id = c.circ_id).first()
      if not circ: return # Skip circuits from before we came online

      circ.expunge()
      # Convert to built circuit
      Circuit.table.update(Circuit.id ==
                circ.id).execute(row_type='builtcircuit')
      circ = BuiltCircuit.query.filter_by(id=circ.id).one()
      
      circ.built_time = c.arrived_at
      circ.tot_delta = c.arrived_at - circ.launch_time
      tc_session.add(circ)
      tc_session.commit()
    elif c.status == "CLOSED":
      circ = BuiltCircuit.query.filter_by(circ_id = c.circ_id).first()
      if circ:
        circ.expunge()
        if lreason in ("REQUESTED", "FINISHED", "ORIGIN"):
          # Convert to closed circuit
          Circuit.table.update(Circuit.id ==
                    circ.id).execute(row_type='closedcircuit')
          circ = ClosedCircuit.query.filter_by(id=circ.id).one()
          circ.closed_time = c.arrived_at
        else:
          # Convert to destroyed circuit
          Circuit.table.update(Circuit.id ==
                    circ.id).execute(row_type='destroyedcircuit')
          circ = DestroyedCircuit.query.filter_by(id=circ.id).one()
          circ.destroy_reason = reason
          circ.destroy_time = c.arrived_at
        tc_session.add(circ)
        tc_session.commit()

class StreamListener(CircuitListener):
  def stream_bw_event(self, s):
    strm = Stream.query.filter_by(strm_id = s.strm_id).first()
    if strm:
      plog("DEBUG", "Got stream bw: "+str(s.strm_id))
      strm.tot_read_bytes += s.bytes_read
      strm.tot_write_bytes += s.bytes_written
      tc_session.add(strm)
      tc_session.commit()
 
  def stream_status_event(self, s):
    if s.reason: lreason = s.reason
    else: lreason = "NONE"
    if s.remote_reason: rreason = s.remote_reason
    else: rreason = "NONE"

    if s.status in ("NEW", "NEWRESOLVE"):
      strm = Stream(strm_id=s.strm_id, tgt_host=s.target_host, 
                    tgt_port=s.target_port, init_status=s.status,
                    tot_read_bytes=0, tot_write_bytes=0)
      tc_session.add(strm)
      tc_session.commit()
      return

    strm = Stream.query.filter_by(strm_id = s.strm_id).first()
    if self.track_parent and s.strm_id not in self.parent_handler.streams:
      if strm:
        tc_session.delete(strm)
        tc_session.commit()
      return # Ignore streams that aren't ours

    if not strm: 
      plog("NOTICE", "Ignoring prior stream "+str(s.strm_id))
      return # Ignore prior streams

    reason = s.event_name+":"+s.status+":"+lreason+":"+rreason+":"+strm.init_status

    if s.status == "SENTCONNECT":
      # New circuit
      strm.circuit = Circuit.query.filter_by(circ_id=s.circ_id).first()
      if not strm.circuit:
        plog("NOTICE", "Ignoring prior stream "+str(strm.strm_id)+" with old circuit "+str(s.circ_id))
        tc_session.delete(strm)
        tc_session.commit()
        return
    else:
      circ = None
      if s.circ_id:
        circ = Circuit.query.filter_by(circ_id=s.circ_id).first()
      elif self.track_parent:
        circ = self.parent_handler.streams[s.strm_id].circ
        if not circ: circ = self.parent_handler.streams[s.strm_id].pending_circ
        if circ:
          circ = Circuit.query.filter_by(circ_id=circ.circ_id).first()

      if not circ:
        plog("WARN", "No circuit for "+str(s.strm_id)+" circ: "+str(s.circ_id))

      if not strm.circuit:
        plog("INFO", "No stream circuit for "+str(s.strm_id)+" circ: "+str(s.circ_id))
        strm.circuit = circ

      # XXX: Verify circ id matches stream.circ
    
    if s.status == "SUCCEEDED":
      strm.start_time = s.arrived_at
      for r in strm.circuit.routers: 
        plog("DEBUG", "Added router "+r.idhex+" to stream "+str(s.strm_id))
        r.streams.append(strm)
        tc_session.add(r)
      tc_session.add(strm)
      tc_session.commit()
    elif s.status == "DETACHED":
      for r in strm.circuit.routers:
        r.detached_streams.append(strm)
        tc_session.add(r)
      #strm.detached_circuits.append(strm.circuit)
      strm.circuit.detached_streams.append(strm)
      strm.circuit.streams.remove(strm)
      strm.circuit = None
      tc_session.add(strm)
      tc_session.commit()
    elif s.status == "FAILED":
      strm.expunge()
      # Convert to destroyed circuit
      Stream.table.update(Stream.id ==
                strm.id).execute(row_type='failedstream')
      strm = FailedStream.query.filter_by(id=strm.id).one()
      strm.fail_time = s.arrived_at
      strm.fail_reason = reason
      tc_session.add(strm)
      tc_session.commit()
    elif s.status == "CLOSED":
      if isinstance(strm, FailedStream):
        strm.close_reason = reason
      else:
        strm.expunge()
        if not (lreason == "DONE" or (lreason == "END" and rreason == "DONE")):
          # Convert to destroyed circuit
          Stream.table.update(Stream.id ==
                    strm.id).execute(row_type='failedstream')
          strm = FailedStream.query.filter_by(id=strm.id).one()
          strm.fail_time = s.arrived_at
        else: 
          # Convert to destroyed circuit
          Stream.table.update(Stream.id ==
                    strm.id).execute(row_type='closedstream')
          strm = ClosedStream.query.filter_by(id=strm.id).one()
          strm.read_bandwidth = strm.tot_read_bytes/(s.arrived_at-strm.start_time)
          strm.write_bandwidth = strm.tot_write_bytes/(s.arrived_at-strm.start_time)
          strm.end_time = s.arrived_at
        strm.close_reason = reason
      tc_session.add(strm)
      tc_session.commit()

def run_example(host, port):
  """ Example of basic TorCtl usage. See PathSupport for more advanced
      usage.
  """
  print "host is %s:%d"%(host,port)
  setup_db("sqlite:///torflow.sqllite")
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.connect((host,port))
  c = Connection(s)
  th = c.launch_thread()
  c.authenticate(control_pass)
  c.set_event_handler(TorCtl.ConsensusTracker(c))
  c.add_event_listener(ConsensusTrackerListener())
  c.add_event_listener(CircuitListener())

  print `c.extend_circuit(0,["moria1"])`
  try:
    print `c.extend_circuit(0,[""])`
  except TorCtl.ErrorReply: # wtf?
    print "got error. good."
  except:
    print "Strange error", sys.exc_info()[0]
   
  c.set_events([EVENT_TYPE.STREAM, EVENT_TYPE.CIRC,
          EVENT_TYPE.NEWCONSENSUS, EVENT_TYPE.NEWDESC,
          EVENT_TYPE.ORCONN, EVENT_TYPE.BW], True)

  th.join()
  return

  
if __name__ == '__main__':
  run_example(control_host,control_port)

