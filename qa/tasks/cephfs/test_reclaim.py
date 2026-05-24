"""
test session reclaim interface of libcephfs
"""

import os
from multiprocessing import Process
import logging
import cephfs
import uuid
from tasks.cephfs.cephfs_test_case import CephFSTestCase

CEPH_RECLAIM_RESET = 1

log = logging.getLogger(__name__)

def dying_client(client_uuid):
        # NOTE: call with a fork()
        # create a session with uuid + session timeout set to 300 seconds
        c1 = cephfs.LibCephFS(conffile='')
        c1.init()
        c1.set_session_timeout(300)
        try:
            c1.start_reclaim(client_uuid, CEPH_RECLAIM_RESET)
            assert False, f"start_reclaim should have raised -ENOENT (uuid={client_uuid})"
        except cephfs.ObjectNotFound:
            pass
        c1.set_uuid(client_uuid)
        c1.mount()
        # do some i/o to generate journal event
        c1.mkdir('/test_reclaim', 0o755)
        fd = c1.open(f'/test_reclaim/file_{client_uuid}', os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o755)
        c1.write(fd, b'test data', 0)
        c1.fsync(fd, 0)
        # die without cleanup
        os._exit(0)

class TestSessionReclaim(CephFSTestCase):
    MDSS_REQUIRED  = 2
    CLIENTS_REQUIRED = 0 # we manage our own libcephfs clients
        

    def test_reclaim_after_mds_failover(self):
        """
        That session reclaim succeeds after an MDS failover. (auth_name must
        be preserved after journal replay).
        """
        c1_uuid = str(uuid.uuid4())

        p = os.fork()
        if p == 0:
            dying_client(c1_uuid)
            os._exit(1) # to catch dying_client not exiting with 0
        else:
            _, status = os.waitpid(p, 0)
            self.assertTrue(os.WIFEXITED(status), "dying_client did not exit normally")
            self.assertEqual(os.WEXITSTATUS(status), 0, "dying_client failed")

        # fail the mds to enforce new active MDS read session from journaled
        # ESession events
        self.fs.mds_fail_restart()
        self.fs.wait_for_state('up:clientreplay', timeout=60, rank=0)

        c2 = cephfs.LibCephFS(conffile='')
        c2.init()
        c2.set_session_timeout(300)
        self.assertEqual(c2.start_reclaim(c1_uuid, CEPH_RECLAIM_RESET), 0)
        c2.finish_reclaim()
        c2.set_uuid(c1_uuid)
        self.fs.wait_for_state('up:active', timeout=60, rank=0)
        c2.mount()
        c2.stat(f'/test_reclaim/file_{c1_uuid}')
        c2.shutdown()
