# coding: utf-8
from antelope_core.providers import XdbClient
from antelope_manager.authorization.xdb_tokens import create_jwt_payload, issue_signed_token
from antelope_manager.authorization import open_private_key
from antelope_core.auth import AuthorizationGrant
ag = [
AuthorizationGrant(user='brandon', origin='ecoinvent.3.7.1.cutoff', access='index', values=False, update=False),
AuthorizationGrant(user='brandon', origin='ecoinvent.3.7.1.cutoff', access='exchange', values=True, update=False),
AuthorizationGrant(user='brandon', origin='ecoinvent.3.7.1.cutoff', access='background', values=True, update=False)
]
from antelope_manager.authorization import open_private_key, MASTER_ISSUER
payload = create_jwt_payload(user='brandon', issuer=MASTER_ISSUER, grants=ag)
key = open_private_key()
tok = issue_signed_token(payload.dict(), key)
xdbc = XdbClient('http://localhost:8001', ref='ecoinvent.3.7.1.cutoff', token=tok)
