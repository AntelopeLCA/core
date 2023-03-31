from pydantic import BaseModel
from typing import List
from antelope.xdb_tokens import JwtGrant


class AuthModel(BaseModel):
    pass


JWT_SCOPES = {
    'bas': 'basic',
    'ind': 'index',
    'exc': 'exchange',
    'bac': 'background',
    'qua': 'quantity',
    'for': 'foreground'
}


class AuthorizationGrant(AuthModel):
    """
    This class stands alone as a list of authorizations granted to users. There is no requirement that the
    users exist in a database

    One natural way to use this is to grant users JWTs using an oauth2 system- these could be provided by a separate
    auth server, or by our xdb itself.  Right now I think I will write a client-side application that will generate
    a valid JWT and simply pass it. Then the JWT will contain the embedded user name, and this rinky-dink table
    will specify what resources that user has permission to access.

    The grown-up way to use it is for the bearer token to be created + signed by an auth server, and to include
    the grants embedded in the token. The problem is that this would become cumbersome if the user had access to
    a great many origins (e.g. this doesn't fly for user foreground grants-- but-- )

    Hold on. The way we are planning on doing this is embedding the token in the RESOURCE, so that the client
    simply passes it- in which case every resource would have its own token.  AND the tokens would not be user-
    specific.  Basically it becomes not the xdb's job to check the user's identity, only to validate the token
    and confirm that the token provides the requested access.  (and log and meter the users' requests)

    This doesn't seem very secure. We should want the token to be limited to one user.  But of course, that is how
    bearer auth works. You assume the authenticated user doesn't distribute the token. Anyway, it's moot if you are
    billing the user for its usage.
    """
    user: str  #
    origin: str  #
    issuer: str = None  # a query for a given origin must be signed by the issuer; none is permitted for antelope data
    # access: bool    # access the origin- this could be said to be implicitly true
    access: str       # instead report the interface being accessed. single interface only
    values: bool = False    # whether numeric data is authorized
    update: bool = False    # whether the user can POST to the resource

    def serialize(self):
        """ # old spec from blackbook draft
        grants are serialized as ' '-delimited access specifications of the form:

        origin.dot.separated[:interface[,f]*]

        interfaces only need to be three-letter, according to:
         bas-ic
         ind-ex
         exc-hange
         bac-kground
         qua-ntity
         for-eground

        ,-separated Flags follow:
         v - values
         w - writable
         m - metered

        The origin 'qdb' is special and refers to antelope cloud qdb and is short for "qdb:basic:quantity,v"
        (lots of DNS TXT record vibes here)

        An example of a verbose, precise specification would be:

        antelope: "ecoinvent.3.6.cut-off:basic:index:exchange,v:background,v,m qdb:quantity,v"

        An example of a terse, broad specification would be:

        antelope: "ecoinvent:bas:ind:exc,v:bac,v,m qdb"

        The __str__ function for Grant objects the components between the colons in canonical (2-letter) form
        """
        grant = self.access
        if self.values:
            grant += ',v'
        if self.update:
            grant += ',u'
        return grant

    @property
    def display(self):
        return '%s:%s:%s' % (self.user, self.origin, self.serialize())

    @classmethod
    def from_jwt(cls, jwt: JwtGrant) -> List:
        """
        returns a list of grants
        NOTE: This does not currently deal with quota flags
        :param jwt:
        :return:
        """
        user = jwt.sub
        grants = []
        for grant in jwt.grants.split(' '):
            clauses = grant.split(':')
            origin = clauses[0]
            ifaces = clauses[1:]
            if origin == 'qdb' and len(ifaces) == 0:
                origin = 'local.qdb'
                ifaces = ['basic', 'quantity,v']
            for iface in ifaces:
                specs = iface.split(',')
                access = JWT_SCOPES[specs[0][:3]]
                values = 'v' in specs
                update = 'u' in specs

                grants.append(cls(user=user, origin=origin, access=access, values=values, update=update))

        return grants
