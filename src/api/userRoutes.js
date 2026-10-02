const registerUserApiRoutes = ({
  app,
  requireApiUserAuth,
  db,
  normalizeLoyaltyPoints,
  getUnreadNotificationCount,
  maskWalletCardNumber,
  sanitizeWalletCardSpendingLimit,
  buildSessionUser
}) => {
  app.get('/api/me', requireApiUserAuth, (req, res) => {
    const users = db.users();
    const user = users.find((item) => item.id === req.apiUser.id);
    if (!user) return res.status(404).json({ error: 'User not found' });
    res.json({
      user: {
        id: user.id,
        name: user.name,
        email: user.email,
        role: user.role,
        walletBalance: Number(user.walletBalance || 0),
        loyaltyPoints: normalizeLoyaltyPoints(user.loyaltyPoints),
        referralCode: user.referralCode || null,
        unreadNotificationsCount: getUnreadNotificationCount(user),
        walletCardNumberMasked: maskWalletCardNumber(user.walletCardNumber),
        walletCardSpendingLimit: sanitizeWalletCardSpendingLimit(user.walletCardSpendingLimit),
        subscription: buildSessionUser(user).subscription
      }
    });
  });
};

module.exports = { registerUserApiRoutes };
