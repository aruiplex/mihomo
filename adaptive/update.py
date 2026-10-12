#!/usr/bin/env python3
"""Merge a formal stable upstream tag on an isolated upgrade branch."""
import argparse
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def call(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tag', required=True)
    args=parser.parse_args()
    if not re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', args.tag): parser.error('only formal semver tags are accepted')
    release=json.loads(call('gh','api','repos/MetaCubeX/mihomo/releases/tags/'+args.tag))
    if release.get('draft') or release.get('prerelease'): raise ValueError('upstream release is not formal stable')
    path=ROOT/'adaptive/baseline.json'; baseline=json.loads(path.read_text())
    if args.tag == baseline['upstream_tag']: print('already current'); return
    old=tuple(map(int,baseline['upstream_tag'][1:].split('.')))
    if tuple(map(int,args.tag[1:].split('.'))) <= old: raise ValueError('upgrade must advance the upstream version')
    branch='adaptive/upgrade-'+args.tag
    existing=json.loads(call('gh','pr','list','--repo','aruiplex/mihomo','--head',branch,'--state','open','--json','number'))
    if existing:
        print('upgrade already proposed; existing PR '+str(existing[0]['number']))
        return
    call('git','fetch','https://github.com/MetaCubeX/mihomo.git','refs/tags/'+args.tag+':refs/tags/'+args.tag)
    call('git','checkout','-b',branch)
    result=subprocess.run(['git','merge','--no-edit',args.tag],cwd=ROOT,capture_output=True,text=True)
    if result.returncode:
        report={'tag':args.tag,'branch':branch,'base_commit':call('git','rev-parse','HEAD'),
                'status':call('git','status','--porcelain'),'diff':call('git','diff'),
                'message':'upstream merge failed; patch is retained; resolve the captured conflict before retrying'}
        (ROOT/'upgrade-failure.json').write_text(json.dumps(report,indent=2)+'\n')
        raise RuntimeError('upstream merge conflict; see upgrade-failure.json and uploaded workspace')
    baseline.update(upstream_tag=args.tag,upstream_commit=call('git','rev-parse',args.tag+'^{commit}'),version=args.tag+'-adaptive.1')
    path.write_text(json.dumps(baseline,indent=2)+'\n')
    call('git','add',str(path)); call('git','commit','-m','chore: validate adaptive routing on '+args.tag)
    call('git','push','origin',branch)
    body=ROOT/'upgrade-pr.md'; body.write_text('Merge upstream '+args.tag+' while retaining adaptive routing and local quality history.\n\nThe candidate must pass race, configuration, verified TLS stream/idle, failure/reload and protocol no-replay checks. CI artifacts are isolated candidates; local activation remains a separate maintenance action.\n')
    call('gh','pr','create','--repo','aruiplex/mihomo','--base','adaptive/stable','--head',branch,
         '--title','Upgrade adaptive routing to '+args.tag,'--body-file',str(body))
    # GITHUB_TOKEN-created pull requests do not trigger PR workflows; explicit
    # workflow_dispatch does, so this is required to run acceptance for the branch.
    call('gh','workflow','run','adaptive-ci.yml','--repo','aruiplex/mihomo','--ref',branch)


if __name__=='__main__': main()
