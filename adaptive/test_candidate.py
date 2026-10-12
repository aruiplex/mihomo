"""A failed build or unpinned toolchain must never yield a usable manifest."""
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

spec=importlib.util.spec_from_file_location('validate',Path(__file__).with_name('validate.py'))
validate=importlib.util.module_from_spec(spec); spec.loader.exec_module(validate)

class CandidateGate(unittest.TestCase):
    def exercise(self, mismatch=False):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); source=root/'source'; fixture=root/'proxy'; bin=root/'bin'
            for directory in [source,fixture,bin]: directory.mkdir()
            def git(directory,*args): return subprocess.check_output(['git',*args],cwd=directory,text=True,stderr=subprocess.DEVNULL).strip()
            for directory in [source,fixture]:
                git(directory,'init'); git(directory,'config','user.name','Fixture'); git(directory,'config','user.email','fixture@invalid')
                (directory/'fixture.txt').write_text('fixture'); git(directory,'add','.'); git(directory,'commit','-m','base')
            git(source,'tag','v1.0.0'); upstream=git(source,'rev-parse','HEAD')
            (source/'adaptive').mkdir()
            (source/'adaptive/baseline.json').write_text(json.dumps({'upstream_tag':'v1.0.0','upstream_commit':upstream,'go_toolchain':'go1.27.2'}))
            (source/'adaptive/fixtures.json').write_text(json.dumps({'commit':git(fixture,'rev-parse','HEAD'), 'files':{'fixture.txt':hashlib.sha256(b'fixture').hexdigest()}}))
            git(source,'add','.'); git(source,'commit','-m','pins')
            go=bin/'go'; go.write_text('#!/usr/bin/python3\nimport sys\nif sys.argv[1:]==["env","GOVERSION"]: print('+repr('go1.0.0' if mismatch else 'go1.27.2')+')\nelif sys.argv[1:]==["env","GOOS","GOARCH"]: print("linux\\namd64")\nelse: raise SystemExit(23)\n'); go.chmod(0o755)
            old_root=validate.ROOT; old_path=os.environ['PATH']; old_argv=sys.argv
            validate.ROOT=source; os.environ['PATH']=str(bin)+os.pathsep+old_path
            sys.argv=['validate.py','--proxy',str(fixture),'--output',str(root/'candidate')]
            try:
                with self.assertRaises(ValueError if mismatch else subprocess.CalledProcessError): validate.main()
                self.assertFalse((root/'candidate/manifest.json').exists())
                self.assertFalse((root/'candidate/manifest.local.json').exists())
            finally: validate.ROOT=old_root; os.environ['PATH']=old_path; sys.argv=old_argv

    def test_failed_race_gate_never_creates_release_manifest(self): self.exercise()
    def test_unpinned_toolchain_never_creates_release_manifest(self): self.exercise(True)

if __name__=='__main__': unittest.main()
