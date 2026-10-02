const jwt = require('jsonwebtoken');

const createApiAuth = ({ jwtSecret, jwtExpiresIn }) => {
  const generateToken = (user) => {
    const payload = {
      id: user.id,
      name: user.name,
      email: user.email,
      role: user.role
    };
    return jwt.sign(payload, jwtSecret, { expiresIn: jwtExpiresIn });
  };

  const verifyToken = (token) => {
    try {
      return jwt.verify(token, jwtSecret);
    } catch (e) {
      return null;
    }
  };

  const requireApiUserAuth = (req, res, next) => {
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({ error: 'Missing or invalid token' });
    }
    const token = authHeader.split(' ')[1];
    const decoded = verifyToken(token);
    if (!decoded) {
      return res.status(401).json({ error: 'Invalid token' });
    }
    if (decoded.role !== 'user') {
      return res.status(403).json({ error: 'API for users only' });
    }
    req.apiUser = decoded;
    next();
  };

  return {
    generateToken,
    verifyToken,
    requireApiUserAuth
  };
};

module.exports = { createApiAuth };
