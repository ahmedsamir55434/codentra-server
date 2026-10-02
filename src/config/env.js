const getTrimmedEnv = (name, fallback = '') => String(process.env[name] || fallback).trim();

const buildEnv = () => {
  const sessionSecret = process.env.SESSION_SECRET || 'codentra-secret-key-2024';
  const authCookieMaxAge = 1000 * 60 * 60 * 24 * 7;

  return {
    port: process.env.PORT || 3000,
    sessionSecret,
    authCookieName: 'codentra_auth',
    authCookieMaxAge,
    databaseUrl: getTrimmedEnv('DATABASE_URL'),
    geminiApiKey: getTrimmedEnv('GEMINI_API_KEY'),
    geminiModel: getTrimmedEnv('GEMINI_MODEL', 'gemini-2.5-flash') || 'gemini-2.5-flash',
    livekitUrl: getTrimmedEnv('LIVEKIT_URL'),
    livekitApiKey: getTrimmedEnv('LIVEKIT_API_KEY'),
    livekitApiSecret: getTrimmedEnv('LIVEKIT_API_SECRET'),
    googleClientId: getTrimmedEnv('GOOGLE_CLIENT_ID'),
    googleClientSecret: getTrimmedEnv('GOOGLE_CLIENT_SECRET'),
    githubClientId: getTrimmedEnv('GITHUB_CLIENT_ID'),
    githubClientSecret: getTrimmedEnv('GITHUB_CLIENT_SECRET'),
    jwtSecret: process.env.JWT_SECRET || 'codentra-jwt-secret-2024',
    jwtExpiresIn: process.env.JWT_EXPIRES_IN || '7d',
    atlosApiUrl: getTrimmedEnv('ATLOS_API_URL', 'https://api.atlos.io/gateway/rest').replace(/\/+$/, ''),
    atlosMerchantId: getTrimmedEnv('ATLOS_MERCHANT_ID', process.env.ATLOS_API_KEY || ''),
    atlosApiSecret: getTrimmedEnv('ATLOS_API_SECRET'),
    atlosWebhookSecret: getTrimmedEnv('ATLOS_WEBHOOK_SECRET'),
    fxApiBaseUrl: getTrimmedEnv('FX_API_BASE_URL', 'https://api.frankfurter.dev/v1').replace(/\/+$/, ''),
    fallbackEgpToUsdRate: Number(process.env.FALLBACK_EGP_TO_USD_RATE || 0.02),
    isVercel: Boolean(process.env.VERCEL),
    authCookieSecret: process.env.AUTH_COOKIE_SECRET || `${sessionSecret}-auth`
  };
};

module.exports = { buildEnv };
