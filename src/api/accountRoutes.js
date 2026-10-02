const registerAccountReadApiRoutes = ({
  app,
  requireApiUserAuth,
  db,
  ensureUserPaymentProfile,
  serializeNotificationItem,
  getNotificationCenterItems,
  getUnreadNotificationCount,
  markAllNotificationsAsRead,
  getLoyaltyRedeemSettings,
  normalizeLoyaltyPoints,
  getUserRealtimeSummary
}) => {
  app.get('/api/purchases', requireApiUserAuth, (req, res) => {
    const purchases = db.purchases().filter((purchase) => purchase.userId === req.apiUser.id);
    const invoices = db.invoices();
    res.json({ purchases, invoices });
  });

  app.get('/api/notifications', requireApiUserAuth, (req, res) => {
    const users = db.users();
    const userIndex = users.findIndex((item) => item && item.id === req.apiUser.id && item.role === 'user');
    if (userIndex === -1) return res.status(404).json({ error: 'User not found' });

    const currentUser = users[userIndex];
    if (ensureUserPaymentProfile({ user: currentUser, users })) {
      db.saveUsers(users);
    }

    if (String(req.query.markRead || '').trim() === '1') {
      if (markAllNotificationsAsRead(currentUser)) {
        db.saveUsers(users);
      }
    }

    return res.json({
      notifications: getNotificationCenterItems(currentUser).map(serializeNotificationItem).filter(Boolean),
      unreadCount: getUnreadNotificationCount(currentUser)
    });
  });

  app.get('/api/loyalty', requireApiUserAuth, (req, res) => {
    const users = db.users();
    const currentUser = users.find((item) => item && item.id === req.apiUser.id && item.role === 'user');
    if (!currentUser) return res.status(404).json({ error: 'User not found' });

    const redeem = getLoyaltyRedeemSettings();
    return res.json({
      loyaltyPoints: normalizeLoyaltyPoints(currentUser.loyaltyPoints),
      walletBalance: Number(currentUser.walletBalance || 0),
      minPoints: normalizeLoyaltyPoints(redeem.minPoints),
      egpPerPoint: Number(redeem.egpPerPoint || 0)
    });
  });

  app.get('/api/live/summary', requireApiUserAuth, (req, res) => {
    return res.json({
      summary: getUserRealtimeSummary({ userId: req.apiUser.id }),
      serverTime: Date.now()
    });
  });
};

module.exports = { registerAccountReadApiRoutes };
