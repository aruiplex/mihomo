#!/usr/bin/env python3
"""Restart the real kernel with private quality state and changed node credentials."""
import argparse, contextlib, json, tempfile, sys, time
from pathlib import Path


def check(binary, fixtures):
    sys.path.insert(0, str(fixtures / 'tests/integration'))
    from protocol_probe import certificate, origin
    from routing_probe import FixtureProxy, Kernel, Stream, serving
    with contextlib.ExitStack() as stack:
        directory=Path(stack.enter_context(tempfile.TemporaryDirectory(prefix='quality-persistence-')))
        _,_,tls_server,tls_client,_=certificate(directory)
        encrypted=origin(stack,tls_server); health=origin(stack)
        nodes={name:stack.enter_context(serving(FixtureProxy(encrypted.server_address,health.server_address))) for name in ['nodeA','nodeB']}
        options={'quality-persistence':True,'quality-ttl':86400,'failure-cooldown':30000}
        target=f'stream.test:{encrypted.server_address[1]}'
        def start(definitions=None):
            result=Kernel(str(binary),'adaptive',directory,encrypted,health,nodes,5,options,proxy_definitions=definitions)
            stack.callback(result.stop)
            return result
        def stream(kernel):
            result=Stream(kernel.port,encrypted.server_address[1],tls_client); stack.callback(result.close); return result
        def chosen(kernel):
            rows=kernel.api('/proxies/TEST/quality')['records']
            return max(rows,key=lambda r:r['selection'])['node']
        first=start(); original=stream(first)
        first.api('/proxies/TEST/feedback','POST',{'target':target,'reason':'timeout'})
        checks={'enabled_via_real_config':first.api('/proxies/TEST/quality')['persistence'] is True,
                'feedback_keeps_existing_verified_stream':original.progressing()}
        first.stop()
        restarted=start(); resumed=stream(restarted)
        checks['restart_restores_target_feedback']=chosen(restarted)=='nodeB' and resumed.progressing()
        restarted.stop()
        definitions=[{'type':'http','name':name,'server':'127.0.0.1','port':node.server_address[1],'username':'fixture','password':'changed'} for name,node in nodes.items()]
        changed=start(definitions); changed_stream=stream(changed)
        checks['same_name_credentials_change_invalidates_old_feedback']=chosen(changed)=='nodeA' and changed_stream.progressing()
        changed.stop()
        files=list((directory/'adaptive-quality').glob('*.json'))
        checks['checkpoint_is_private_and_bounded']=bool(files) and all(p.stat().st_mode & 0o777 == 0o600 and p.stat().st_size < 1048576 for p in files)
        for path in files: path.write_text('corrupt json')
        cold=start(definitions); fresh=stream(cold)
        checks['corrupt_state_cold_starts_without_stopping_service']=fresh.progressing()
        return {'checks':checks,'scope':'isolated real core restarts and node credential changes, verified fixture TLS; no production changes'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kernel',type=Path,required=True); parser.add_argument('--fixtures',type=Path,required=True); parser.add_argument('--output',type=Path)
    args=parser.parse_args(); report=check(args.kernel.resolve(),args.fixtures.resolve()); text=json.dumps(report,indent=2)
    if args.output: args.output.write_text(text+'\n')
    print(text); return 0 if all(report['checks'].values()) else 1

if __name__=='__main__': raise SystemExit(main())
