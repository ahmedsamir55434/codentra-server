const buildApiSessionUser = (apiUser) => ({
  role: 'user',
  id: apiUser.id
});

const registerCartReadApiRoutes = ({
  app,
  requireApiUserAuth,
  getOrCreateCartForUser,
  normalizeCouponCode,
  summarizeCart
}) => {
  app.get('/api/cart', requireApiUserAuth, (req, res) => {
    const { cart } = getOrCreateCartForUser({ userId: req.apiUser.id });
    res.json({ cart });
  });

  app.get('/api/cart/summary', requireApiUserAuth, (req, res) => {
    const { cart } = getOrCreateCartForUser({ userId: req.apiUser.id });
    const couponCode = normalizeCouponCode(req.query.couponCode);
    const summary = summarizeCart({
      cart,
      couponCode,
      sessionUser: buildApiSessionUser(req.apiUser)
    });
    res.json({ summary });
  });
};

module.exports = { registerCartReadApiRoutes };
