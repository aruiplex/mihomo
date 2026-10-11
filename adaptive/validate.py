#!/usr/bin/env python3
"""Build and validate this exact checkout, then export a fresh bound manifest."""
import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    value = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1048576), b''): value.update(block)
    return value.hexdigest()


def run(args, cwd=ROOT):
    subprocess.run(list(map(str, args)), cwd=cwd, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--proxy', type=Path, required=True, help='pinned proxy checkout')
    parser.add_argument('--output', type=Path, required=True, help='new output directory')
    args = parser.parse_args()
    baseline = json.loads((ROOT/'adaptive/baseline.json').read_text())
    pin = json.loads((ROOT/'adaptive/fixtures.json').read_text())
    proxy = args.proxy.resolve()
    commit = subprocess.check_output(['git','rev-parse','HEAD'], cwd=proxy, text=True).strip()
    if commit != pin['commit']: raise ValueError('fixture checkout differs from the pinned revision')
    for file, expected in pin['files'].items():
        if digest(proxy/file) != expected: raise ValueError('fixture content differs from its pin')
    if subprocess.check_output(['git','status','--porcelain','--untracked-files=no'], cwd=ROOT, text=True).strip():
        raise ValueError('build requires a clean tracked source checkout')
    source = subprocess.check_output(['git','rev-parse','HEAD'], cwd=ROOT, text=True).strip()
    upstream = subprocess.check_output(['git','rev-parse',baseline['upstream_tag']+'^{commit}'], cwd=ROOT, text=True).strip()
    if upstream != baseline['upstream_commit']: raise ValueError('upstream tag differs from baseline')
    toolchain = subprocess.check_output(['go','env','GOVERSION'], text=True).strip()
    platform = subprocess.check_output(['go','env','GOOS','GOARCH'], text=True).split()
    if toolchain != baseline['go_toolchain'] or '/'.join(platform) != 'linux/amd64':
        raise ValueError('only pinned toolchain on linux/amd64 is currently supported')
    output = args.output.resolve(); output.mkdir(parents=True, exist_ok=False)
    tests = proxy/'tests/integration'
    run(['go','test','-race','./adapter/outboundgroup','./adapter','-run','Adaptive|RoutingIdentity','-count=1'])
    run(['go','test','./adapter/outboundgroup','./adapter','./hub/route','./tunnel','./config','./constant'])
    binary = output / f'mihomo-linux-amd64-{baseline["version"]}'
    run(['go','build','-buildvcs=false','-trimpath','-ldflags',
         '-X github.com/metacubex/mihomo/constant.Version='+baseline['version'],'-o',binary,'.'])
    binary_sha = digest(binary)
    official_source = output/'upstream-source'
    run(['git','worktree','add','--detach',official_source,upstream])
    official = output/'official-fixture'
    try:
        run(['go','build','-buildvcs=false','-trimpath','-ldflags',
             '-X github.com/metacubex/mihomo/constant.Version='+baseline['upstream_tag'],'-o',official,'.'], official_source)
    finally:
        run(['git','worktree','remove','--force',official_source])
    records = []
    def probe(name, script, extra):
        path = output/(name+'.json')
        run(['python3',tests/script,'--kernel',binary,*extra,'--output',path])
        report = json.loads(path.read_text())
        if not report.get('checks') or not all(v is True for v in report['checks'].values()):
            raise ValueError('fixture requirements failed: '+name)
        if digest(binary) != binary_sha: raise ValueError('binary changed during validation')
        records.append({'name':name,'sha256':digest(path),'checks':len(report['checks']), 'binary_sha256':binary_sha})
    probe('managed','managed_routing_probe.py',['--official',official])
    probe('stability','stability_probe.py',[])
    for protocol in ['ss','vmess','vmess-tls','vless','vless-tls','trojan','vmess-ws-early','vless-ws-early']:
        probe(protocol,'protocol_probe.py',['--server-kernel',official,'--protocol',protocol])
    for protocol in ['vmess-ws-early','vless-ws-early']:
        for repeat in range(2):
            probe(protocol+'-ws-reject-'+str(repeat),'protocol_probe.py',
                  ['--server-kernel',official,'--protocol',protocol,'--failure-mode','ws-reject'])
    validation = {'schema':1,'passed':True,'binary_sha256':binary_sha,'source_commit':source,
        'upstream_commit':upstream,'go_toolchain':toolchain,'platform':'linux/amd64',
        'fixture_commit':commit,'reports':records,
        'scope':'isolated verified TLS/TCP fixtures; no production, authenticated AI, TUN, UDP or QUIC acceptance'}
    validation_path = output/'validation.json'; validation_path.write_text(json.dumps(validation,indent=2)+'\n')
    patch = subprocess.check_output(['git','diff',upstream,source,'--','adapter','constant','hub','tunnel'],cwd=ROOT)
    (output/'adaptive.patch').write_bytes(patch)
    manifest = {'schema':1,'source':{'id':baseline['source_id'],'kind':'custom'},'version':baseline['version'],
        'platform':{'os':'linux','arch':'amd64'},
        'asset':{'path':str(binary),'sha256':binary_sha,'compression':'none'},
        'build':{'source_repository':'aruiplex/mihomo','source_commit':source,'upstream_tag':baseline['upstream_tag'],
                 'upstream_commit':upstream,'patch_sha256':hashlib.sha256(patch).hexdigest(),
                 'go_toolchain':toolchain,'validation_sha256':digest(validation_path),'fixture_commit':commit}}
    (output/'manifest.local.json').write_text(json.dumps(manifest,indent=2)+'\n')
    # URL is only usable after explicit release publication. Source identity stays
    # compatible with the existing local prototype; no silent source migration.
    manifest['asset'].pop('path')
    manifest['asset']['url']='https://github.com/aruiplex/mihomo/releases/download/'+baseline['version']+'/'+binary.name
    (output/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    files = [binary, validation_path, output/'adaptive.patch', output/'manifest.json']
    (output/'SHA256SUMS').write_text(''.join(digest(file)+'  '+file.name+'\n' for file in files))
    print(json.dumps({'version':baseline['version'],'binary_sha256':binary_sha,'reports':len(records),'passed':True}))


if __name__=='__main__': main()
