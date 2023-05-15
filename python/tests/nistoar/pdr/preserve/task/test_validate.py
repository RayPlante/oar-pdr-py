import os, json, pdb, logging, tempfile, zipfile, shutil
from pathlib import Path
import unittest as test

from nistoar.pdr.preserve.task import validate as val, state as st
from nistoar.base import config

pdrdir = Path(__file__).resolve().parents[2] 
storedir = pdrdir / "distrib" / "data"
basedir = pdrdir.parents[3]

port = 9091
baseurl = "http://localhost:{0}/".format(port)

def startService(authmeth=None):
    tdir = tmpdir()
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    wpy = "python/tests/nistoar/pdr/distrib/sim_distrib_srv.py"
    cmd = "uwsgi --daemonize {0} --plugin python3 --http-socket :{1} " \
          "--wsgi-file {2} --pidfile {3}"
    cmd = cmd.format(os.path.join(tdir,"simsrv.log"), srvport,
                     os.path.join(basedir, wpy), pidfile)
    status = os.system(cmd) == 0
    time.sleep(0.5)
    return status

def stopService(authmeth=None):
    tdir = tmpdir()
    srvport = port
    if authmeth == 'header':
        srvport += 1
    pidfile = os.path.join(tdir,"simsrv"+str(srvport)+".pid")
    
    cmd = "uwsgi --stop {0}".format(os.path.join(tdir,
                                                 "simsrv"+str(srvport)+".pid"))
    os.system(cmd)
    time.sleep(1)

tmpdir = tempfile.TemporaryDirectory(prefix="_test_state.")
testbag = Path(tmpdir.name) / "mds2-7223.1_0_0.mbag0_4-0"
loghdlr = None
rootlog = None
def setUpModule():
    global loghdlr
    global rootlog
    rootlog = logging.getLogger()
    loghdlr = logging.FileHandler(os.path.join(tmpdir.name,"test_state.log"))
    loghdlr.setLevel(logging.DEBUG)
    rootlog.addHandler(loghdlr)

    with zipfile.ZipFile(storedir/"mds2-7223.1_0_0.mbag0_4-0.zip") as zip:
        zip.extractall(os.path.join(tmpdir.name))
    # (Path(tmpdir.name)/"mds2-7223.1_0_0.mbag0_4-0").rename(testbag)

def tearDownModule():
    global loghdlr
    if loghdlr:
        if rootlog:
            rootlog.removeHandler(loghdlr)
            loghdlr.flush()
            loghdlr.close()
        loghdlr = None
    tmpdir.cleanup()

class TestNISTBagValidation(test.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory(prefix="work.", dir=tmpdir.name)
        self.cfg = {
            "check_data_files": False,
        }
        self.val = val.NISTBagValidation(self.cfg)
        self.mgr = st.JSONPreservationStateManager({"working_dir": self.tmpdir.name}, "mds2-7223", 
                                                   str(testbag), persistin=Path(self.tmpdir.name))
        self.mgr.set_finalized_aip(str(testbag))

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_ctor(self):
        self.assertEqual(self.val.cfg, self.cfg)
        self.assertEqual(self.mgr.aipid, "mds2-7223")
        self.assertEqual(self.mgr.get_finalized_aip(), str(testbag))

    def test_no_ops(self):
        self.val.revert(self.mgr)
        self.val.clean_up(self.mgr)

    def test_apply(self):
        self.val.apply(self.mgr)
        resfile = Path(self.tmpdir.name)/"validation_results.json"
        self.assertTrue(resfile.is_file())
        with open(resfile) as fd:
            data = json.load(fd)

        self.assertIs(data['is_valid'], True)
        self.assertIn('failed', data)
        self.assertNotIn('passed', data)
        self.assertEqual(len(data['failed']), 0)

    def test_noresults(self):
        self.mgr = st.JSONPreservationStateManager({}, "mds2-7223", str(testbag), clear_state=True,
                                                   persistin=Path(self.tmpdir.name))
        self.mgr.set_finalized_aip(str(testbag))
        self.val.apply(self.mgr)
        resfile = Path(self.tmpdir.name)/"validation_results.json"
        self.assertTrue(not resfile.exists())

    def test_record_passed(self):
        self.val.cfg['record_passed'] = True
        self.val.apply(self.mgr)
        
        resfile = Path(self.tmpdir.name)/"validation_results.json"
        self.assertTrue(resfile.is_file())
        with open(resfile) as fd:
            data = json.load(fd)

        self.assertIs(data['is_valid'], True)
        self.assertIn('failed', data)
        self.assertIn('passed', data)
        self.assertEqual(len(data['failed']), 0)
        

    def test_failure(self):
        bag = Path(self.tmpdir.name)/"mds2-7223"
        shutil.copytree(testbag, bag)
        self.mgr = st.JSONPreservationStateManager({"working_dir": self.tmpdir.name}, "mds2-7223", 
                                                   str(bag), clear_state=True,
                                                   persistin=Path(self.tmpdir.name))
        self.mgr.set_finalized_aip(str(bag))
        with self.assertRaises(val.AIPValidationError):
            self.val.apply(self.mgr)
        
        resfile = Path(self.tmpdir.name)/"validation_results.json"
        self.assertTrue(resfile.is_file())
        with open(resfile) as fd:
            data = json.load(fd)

        self.assertIs(data['is_valid'], False)
        self.assertIn('failed', data)
        self.assertNotIn('passed', data)
        self.assertEqual(len(data['failed']), 1)
        



if __name__ == '__main__':
    test.main()
