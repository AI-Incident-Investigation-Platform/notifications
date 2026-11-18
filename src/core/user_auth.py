from uuid import UUID

import jwt
from aiip_kit.access.model import Principal
from aiip_kit.errors import NotAuthenticatedError
from aiip_kit.service_auth.jwks import KeyResolver
from aiip_kit.service_auth.verifier import bearer


class JwtUserAuthenticator:
    def __init__(self, keys: KeyResolver):
        self.keys = keys

    async def authenticate(self, authorization: str | None) -> Principal:
        token = bearer(authorization)
        if token is None:
            raise NotAuthenticatedError
        try:
            kid = jwt.get_unverified_header(token).get("kid")
            key = await self.keys.get(kid) if isinstance(kid, str) else None
            if key is None:
                raise NotAuthenticatedError
            claims = jwt.decode(
                token,
                key,
                algorithms=["RS256"],
                issuer="auth",
                audience="notifications",
                options={"require": ["iss", "aud", "sub", "sid", "iat", "exp"]},
            )
            if claims.get("token_use") == "service":
                raise NotAuthenticatedError
            return Principal(user_id=UUID(claims["sub"]), session_id=UUID(claims["sid"]))
        except (jwt.PyJWTError, ValueError, TypeError, KeyError) as exc:
            raise NotAuthenticatedError from exc
