import jwt from 'jsonwebtoken';
import type { AuthClaims } from '../types.js';

const SECRET = process.env.JWT_SECRET ?? 'collab-dev-secret-change-me';
const DEFAULT_TTL = '12h';

export function signToken(claims: Omit<AuthClaims, 'iat' | 'exp'>, ttl: string = DEFAULT_TTL): string {
  return jwt.sign(claims, SECRET, { expiresIn: ttl } as jwt.SignOptions);
}

export function verifyToken(token: string): AuthClaims {
  const decoded = jwt.verify(token, SECRET);
  if (typeof decoded === 'string' || !decoded || typeof decoded !== 'object') {
    throw new Error('invalid_token_payload');
  }
  const sub = String((decoded as AuthClaims).sub ?? '');
  const name = String((decoded as AuthClaims).name ?? 'Anonymous');
  if (!sub) throw new Error('missing_sub');
  return { sub, name, iat: (decoded as AuthClaims).iat, exp: (decoded as AuthClaims).exp };
}

/** Dev helper: mint a short-lived demo token when clients omit one. */
export function mintDemoToken(userId?: string, name?: string): string {
  const id = userId ?? `user-${Math.random().toString(36).slice(2, 8)}`;
  return signToken({ sub: id, name: name ?? `Guest ${id.slice(-4)}` });
}
