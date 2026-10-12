"""Exercise upgrade success and conflict at real Git and fixture GitHub seams."""
import contextlib
import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

spec=importlib.util.spec_from_file_location('upgrade',Path(__file__).with_name('update.py'))
upgrade=importlib.util.module_from_spec(spec); spec.loader.exec_module(upgrade)

class StableUpgrade(unittest.TestCase):
    def fixture(self, root, conflict=False):
        upstream=root/'upstream'; upstream.mkdir()
        def git(directory,*args):
            return subprocess.check_output(['git',*args],cwd=directory,text=True,stderr=subprocess.DEVNULL).strip()
        git(upstream,'init'); git(upstream,'config','user.name','Fixture'); git(upstream,'config','user.email','fixture@invalid')
        (upstream/'route.txt').write_text('base\n'); git(upstream,'add','.'); git(upstream,'commit','-m','base'); git(upstream,'tag','v1.0.0')
        (upstream/('route.txt' if conflict else 'new-upstream.txt')).write_text('upstream\n'); git(upstream,'add','.'); git(upstream,'commit','-m','stable update'); git(upstream,'tag','v1.0.1')
        work=root/'work'; git(root,'clone',str(upstream),str(work)); git(work,'checkout','-b','adaptive/stable','v1.0.0')
        git(work,'config','user.name','Fixture'); git(work,'config','user.email','fixture@invalid')
        (work/'adaptive').mkdir(); (work/'adaptive/baseline.json').write_text(json.dumps({'upstream_tag':'v1.0.0','version':'v1.0.0-adaptive.3'}))
        (work/('route.txt' if conflict else 'adaptive.txt')).write_text('patch\n'); git(work,'add','.'); git(work,'commit','-m','adaptive patch')
        remote=root/'origin.git'; git(root,'clone','--bare',str(work),str(remote)); git(work,'remote','set-url','origin',str(remote))
        git(work,'config','url.'+str(upstream)+'.insteadOf','https://github.com/MetaCubeX/mihomo.git')
        bin=root/'bin'; bin.mkdir(); log=root/'calls.jsonl'
        gh=bin/'gh'; gh.write_text("#!/usr/bin/python3\nimport sys,json\nfrom pathlib import Path\nwith Path("+repr(str(log))+").open('a') as out: out.write(json.dumps(sys.argv[1:])+'\\n')\nif sys.argv[1]=='api': print(json.dumps({'draft':False,'prerelease':False}))\nelif sys.argv[1:3]==['pr','list']: print('[]')\nelse: print('fixture')\n"); gh.chmod(0o755)
        return work,log,bin

    def execute(self, work, bin):
        old_root=upgrade.ROOT; old_path=os.environ['PATH']; old_argv=__import__('sys').argv
        upgrade.ROOT=work; os.environ['PATH']=str(bin)+os.pathsep+old_path; __import__('sys').argv=['update.py','--tag','v1.0.1']
        try: upgrade.main()
        finally: upgrade.ROOT=old_root; os.environ['PATH']=old_path; __import__('sys').argv=old_argv

    def test_formal_stable_upgrade_keeps_patch_and_opens_review_before_activation(self):
        with tempfile.TemporaryDirectory() as tmp:
            work,log,bin=self.fixture(Path(tmp)); self.execute(work,bin)
            self.assertEqual((work/'adaptive.txt').read_text(),'patch\n')
            self.assertEqual(json.loads((work/'adaptive/baseline.json').read_text())['upstream_tag'],'v1.0.1')
            calls=[json.loads(line) for line in log.read_text().splitlines()]
            self.assertTrue(any(call[:2]==['pr','create'] for call in calls))
            self.assertTrue(any(call[:2]==['workflow','run'] for call in calls))
            self.assertFalse(any('release' in call for call in calls))

    def test_conflicting_upstream_preserves_failure_evidence_and_never_publishes(self):
        with tempfile.TemporaryDirectory() as tmp:
            work,log,bin=self.fixture(Path(tmp),True)
            with self.assertRaises(RuntimeError): self.execute(work,bin)
            self.assertTrue((work/'upgrade-failure.json').exists())
            self.assertIn('<<<<<<<',(work/'route.txt').read_text())
            self.assertEqual(json.loads((work/'adaptive/baseline.json').read_text())['upstream_tag'],'v1.0.0')
            calls=[json.loads(line) for line in log.read_text().splitlines()]
            self.assertFalse(any(call[:2]==['pr','create'] or 'release' in call for call in calls))

if __name__=='__main__': unittest.main()
