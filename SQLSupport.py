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

from sqlalchemy.orm import scoped_session, sessionmaker, eagerload, lazyload, eagerload_all
from sqlalchemy import create_engine
from sqlalchemy.schema import ThreadLocalMetaData,MetaData
from elixir import *

#################### Model #######################

# In elixir, the session (DB connection) is a property of the model..
# There can only be one for all of the listeners below that use it
# See http://elixir.ematia.de/trac/wiki/Recipes/MultipleDatabases
OP=None
tc_metadata = MetaData()
tc_metadata.echo=False
tc_session = scoped_session(sessionmaker(autoflush=True))
#__metadata__ = tc_metadata
#__session__ = tc_session

def setup_db(db_uri):
  #session.close()
  tc_engine = create_engine(db_uri, echo=False)
  tc_metadata.bind = tc_engine
  tc_metadata.echo = False

  setup_all()
  create_all()

class Router(Entity):
  using_options(order_by='-published', session=tc_session, metadata=tc_metadata)
  using_mapper_options(save_on_init=False)
  idhex = Field(CHAR(40), primary_key=True, index=True)
  orhash = Field(CHAR(27))
  published = Field(Time)
  nick = Field(Text)

  OS = Field(Text)
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
  bw_history = OneToMany('BwHistory')
  stats = OneToOne('RouterStats', inverse="router")

  def from_router(self, router):
    self.published = router.published
    self.bw = router.bw
    self.idhex = router.idhex
    self.orhash = router.orhash
    self.nick = router.nickname
    self.OS = router.os
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
  streams = OneToMany('Stream')
  extensions = OneToMany('Extension', inverse='circ')
  circ_id = Field(Integer, index=True)
  launch_time = Field(Float)
  last_extend = Field(Float)

  def xfer_copy(self, circ):
    rs = []
    for r in circ.routers:
      rs.append(r)
    for r in rs:
      #r.circuits.append(self)
      rcl = len(r.circuits)
      self.routers.append(r)
      r.circuits.remove(circ)
      assert rcl == len(r.circuits)
    assert len(self.routers) == len(rs)

    csl = []
    for s in circ.streams:
      csl.append(s)
    for s in csl:
      self.streams.append(s)
      s.circ = self
    assert len(csl) == len(self.streams)

    cel = []
    for e in circ.extensions:
      cel.append(e)
    for e in cel: 
      self.extensions.append(e)
      e.circ = self
    assert len(cel) == len(self.extensions)

    self.circ_id = circ.circ_id
    self.launch_time = circ.launch_time
    self.last_extend = circ.last_extend
    tc_session.delete(circ)
    return self

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
  circuit = ManyToOne('Circuit')
  strm_id = Field(Integer, index=True)
  start_time = Field(Float)
  tot_bytes = Field(Integer)

class FailedStream(Stream):
  using_options(session=tc_session, metadata=tc_metadata)
  using_mapper_options(save_on_init=False)
  reason = Field(Text)
  fail_time = Field(Float)

class ClosedStream(Stream):
  using_options(session=tc_session, metadata=tc_metadata)
  using_mapper_options(save_on_init=False)
  end_time = Field(Float)
  bandwidth = Field(Float)

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

  circ_to_ratio = Field(Float)
  circ_from_ratio = Field(Float)
  circ_bi_ratio = Field(Float)

  avg_first_ext = Field(Float)
  first_ext_ratio = Field(Float)
  
  avg_sbw = Field(Float)
  sbw_ratio = Field(Float)

  # FIXME: Figure out how to efficiently compute these..
  filt_to_ratio = Field(Float)
  filt_from_ratio = Field(Float)
  filt_bi_ratio = Field(Float)
  filt_sbw_ratio = Field(Float)

  def _compute_stats():
    # FIXME: Change loading method from lazy to eager here
    # FIXME: Always?
    for r in Router.query.all():
      rs = r.stats
      rs.circ_fail_to = 0
      rs.circ_try_to = 0
      rs.circ_fail_from = 0
      rs.circ_try_from = 0
      tot_extend_time = 0
      tot_extends = 0
      for c in r.circuits: 
        # FIXME: Should this be SQL-query based instead?
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
          pass
          # TODO: Also count timeouts against earlier nodes?
        elif isinstance(c, DestroyedCircuit):
          pass # TODO: Count these somehow..

      if rs.circ_try_from > 0:
        rs.circ_from_rate = (1.0*rs.circ_fail_from/rs.circ_try_from)
      if rs.circ_try_to > 0:
        rs.circ_to_rate = (1.0*rs.circ_fail_to/rs.circ_try_to)

      if tot_extends > 0:
        rs.avg_first_ext = (1.0*tot_extend_time)/tot_extends
      else:
        rs.avg_first_ext = 0
      #for s in r.streams:
      #  if isinstance(c, ClosedStream):
      #  elif isinstance(c, FailedStream):
      tc_session.update(rs)
  _compute_stats = Callable(_compute_stats)
 
  def _compute_ranks():
    # FIXME: Single query for this?
    min_avg_rank = 0x7fffffff
    max_avg_rank = 0
    for r in Router.query.all():
      if r.stats: tc_session.delete(r.stats)
      rs = RouterStats()
      rs.router = r
      r.stats = rs

      min_rank = 0x7fffffff
      tot_rank = 0
      tot_bw = 0
      max_rank = 0
      ranks = len(r.bw_history)
      for h in r.bw_history:
        tot_rank += h.rank
        tot_bw += h.bw
        if h.rank < min_rank:
          min_rank = h.rank
        if h.rank > max_rank:
          max_rank = h.rank
      rs.min_rank = min_rank
      rs.avg_rank = (1.0*tot_rank)/ranks
      if rs.avg_rank < min_avg_rank:
        min_avg_rank = rs.avg_rank
      if rs.avg_rank > max_avg_rank:
        max_avg_rank = rs.avg_rank
      rs.max_rank = max_rank
      rs.avg_bw = (1.0*tot_bw)/ranks
      tc_session.save_or_update(rs)
      tc_session.update(r)
    # FIXME: Single query for this?
    for rs in RouterStats.query.all():
      rs.percentile = (100.0*rs.avg_rank)/(max_avg_rank - min_avg_rank)
      tc_session.update(rs)
  _compute_ranks = Callable(_compute_ranks)

  def _compute_ratios():
    pass
  _compute_ratios = Callable(_compute_ratios)

  def _compute_filtered_ratios():
    pass
  _compute_filtered_ratios = Callable(_compute_filtered_ratios)

  def reset():
    for r in Router.query.all():
      r.stats = None
      tc_session.update(r)
    RouterStats.table.drop()
    RouterStats.table.create()
    tc_session.commit()
  reset = Callable(reset)

  def compute(router_filter, stats_filter):
    RouterStats._compute_ranks()
    RouterStats._compute_stats()
    RouterStats._compute_ratios()
    RouterStats._compute_filtered_ratios()
    tc_session.commit()
  compute = Callable(compute)  

##################### End Model ####################

class CircuitStatsBroker:
  pass

class StreamStatsBroker:
  pass

class RatioBroker:
  pass

#################### Model Support ################
def reset_all_stats():
  # Need to keep routers around.. 
  for r in Router.query.all():
    r.bw_history = [] # XXX: Is this sufficient/correct?
    r.circuits = []
    r.streams = []
    r.stats = None
    tc_session.update(r)

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
    self.last_desc_at = time.time()
    self.consensus = None

  # XXX: What about non-running routers and uptime information?
  def _update_rank_history(self, idlist):
    for idhex in idlist:
      if idhex not in self.consensus.routers: continue
      rc = self.consensus.routers[idhex]
      r = Router.query.options(eagerload('bw_history')).filter_by(
                                  idhex=idhex).one()
      bwh = BwHistory(router=r, rank=rc.list_rank, bw=rc.bw, 
                      pub_time=r.published)
      r.bw_history.append(bwh)
      tc_session.save_or_update(bwh)
      tc_session.update(r)
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
        tc_session.save_or_update(r)
    tc_session.commit()

  def update_consensus(self):
    self.consensus = self.parent_handler.current_consensus()
    self._update_db(self.consensus.ns_map.iterkeys())

  def set_parent(self, parent_handler):
    if not isinstance(parent_handler, TorCtl.ConsensusTracker):
      raise TorCtlError("ConsensusTrackerListener can only be attached to ConsensusTracker instances")
    TorCtl.DualEventListener.set_parent(self, parent_handler)

  def heartbeat_event(self, e):
    if e.state == EVENT_STATE.PRELISTEN:
      if not self.consensus: 
        global OP
        OP = Router.query.filter_by(
                 idhex="0000000000000000000000000000000000000000").first()
        if not OP:
          OP = Router(idhex="0000000000000000000000000000000000000000", 
                    orhash="000000000000000000000000000", 
                    nick="!!TorClient", published=datetime.datetime.utcnow())
          tc_session.save_or_update(OP)
          tc_session.commit()
        self.update_consensus()
      # So ghetto
      if e.arrived_at - self.last_desc_at > 20.0:
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
    if self.track_parent and c.cird_id not in self.parent_handler.circuits:
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
          rq.circuits.append(circ)
          tc_session.update(rq)
      tc_session.save_or_update(circ)
      tc_session.commit()
    elif c.status == "EXTENDED":
      circ = Circuit.query.options(eagerload('extensions')).filter_by(
                       circ_id = c.circ_id).first()
      if not circ: return # Skip circuits from before we came online

      e = Extension(circ=circ, hop=len(c.path), time=c.arrived_at)

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
        # XXX: Eager load here?
        circ.routers.append(e.to_node)
        e.to_node.circuits.append(circ)
        tc_session.update(e.to_node)
 
      e.delta = c.arrived_at - circ.last_extend
      circ.last_extend = c.arrived_at
      circ.extensions.append(e)
      tc_session.save_or_update(e)
      tc_session.update(circ)
      tc_session.commit()
    elif c.status == "FAILED":
      circ = Circuit.query.options(eagerload_all('routers.circuits'), 
               eagerload('extensions'), eagerload('streams')).filter_by(
                      circ_id = c.circ_id).first()
      if not circ: return # Skip circuits from before we came online

      if isinstance(circ, BuiltCircuit):
        circ = DestroyedCircuit().xfer_copy(circ) 
        circ.destroy_reason = reason
        circ.destroy_time = c.arrived_at
      else:
        circ = FailedCircuit().xfer_copy(circ)
        circ.fail_reason = reason
        circ.fail_time = c.arrived_at
        e = FailedExtension(circ=circ, hop=len(c.path)+1, time=c.arrived_at)

        if len(c.path) == 0:
          e.from_node = OP
        else:
          r_ext = c.path[-1]
          if r_ext[0] != '$': r_ext = self.parent_handler.name_to_key[r_ext]
 
          e.from_node = Router.query.filter_by(idhex=r_ext[1:]).one()

        if self.track_parent:
          r=self.parent_handler.circuits[c.circ_id].path[len(c.path)+1]
          e.to_node = Router.query.filter_by(idhex=r.idhex).one()
        else:
          e.to_node = None # We have no idea..

        e.delta = c.arrived_at - circ.last_extend
        e.reason = reason
        circ.extensions.append(e)
        circ.fail_time = c.arrived_at
        tc_session.save_or_update(e)

      tc_session.save_or_update(circ)
      tc_session.commit()
    elif c.status == "BUILT":
      circ = Circuit.query.options(eagerload_all('routers.circuits'), 
               eagerload('extensions'), eagerload('streams')).filter_by(
                     circ_id = c.circ_id).first()
      if not circ: return # Skip circuits from before we came online
      
      circ = BuiltCircuit().xfer_copy(circ)
      circ.built_time = c.arrived_at
      circ.tot_delta = c.arrived_at - circ.launch_time
      tc_session.save_or_update(circ)
      tc_session.commit()
    elif c.status == "CLOSED":
      circ = BuiltCircuit.query.options(eagerload_all('routers.circuits'), 
               eagerload('extensions'), eagerload('streams')).filter_by(
                   circ_id = c.circ_id).first()
      if circ:
        if lreason in ("REQUESTED", "FINISHED", "ORIGIN"):
          circ = ClosedCircuit().xfer_copy(circ)
          circ.closed_time = c.arrived_at
        else:
          circ = DestroyedCircuit().xfer_copy(circ)
          circ.destroy_reason = reason
          circ.destroy_time = c.arrived_at
        tc_session.save_or_update(circ)
        tc_session.commit()

class StreamListener(CircuitListener):
  def stream_bw_event(self, s):
    pass
  def stream_status_event(self, s):
    pass

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

