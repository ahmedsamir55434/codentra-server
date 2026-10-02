const findApiUserIndex = ({ users, apiUserId }) => {
  return users.findIndex((item) => item && item.id === apiUserId && item.role === 'user');
};

const registerDeviceApiRoutes = ({ app, requireApiUserAuth, db }) => {
  app.post('/api/device/register', requireApiUserAuth, (req, res) => {
    const pushToken = String(req.body.pushToken || '').trim();
    const platform = String(req.body.platform || 'ios').trim() || 'ios';
    if (!pushToken) return res.status(400).json({ error: 'pushToken is required' });

    const users = db.users();
    const userIndex = findApiUserIndex({ users, apiUserId: req.apiUser.id });
    if (userIndex === -1) return res.status(404).json({ error: 'User not found' });

    const devices = Array.isArray(users[userIndex].pushDevices) ? users[userIndex].pushDevices : [];
    const existingIndex = devices.findIndex((item) => item && item.pushToken === pushToken);
    const payload = {
      pushToken,
      platform,
      updatedAt: new Date().toISOString()
    };

    if (existingIndex === -1) {
      devices.push(payload);
    } else {
      devices[existingIndex] = { ...devices[existingIndex], ...payload };
    }

    users[userIndex].pushDevices = devices;
    db.saveUsers(users);
    return res.json({ success: true });
  });

  app.post('/api/device/unregister', requireApiUserAuth, (req, res) => {
    const pushToken = String(req.body.pushToken || '').trim();
    if (!pushToken) return res.status(400).json({ error: 'pushToken is required' });

    const users = db.users();
    const userIndex = findApiUserIndex({ users, apiUserId: req.apiUser.id });
    if (userIndex === -1) return res.status(404).json({ error: 'User not found' });

    const devices = Array.isArray(users[userIndex].pushDevices) ? users[userIndex].pushDevices : [];
    users[userIndex].pushDevices = devices.filter((item) => item && item.pushToken !== pushToken);
    db.saveUsers(users);
    return res.json({ success: true });
  });
};

module.exports = { registerDeviceApiRoutes };
