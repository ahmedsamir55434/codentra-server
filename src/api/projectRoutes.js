const buildApiSessionUser = (apiUser) => ({
  role: 'user',
  id: apiUser.id
});

const registerProjectApiRoutes = ({
  app,
  requireApiUserAuth,
  db,
  isProjectVisibleToUser,
  decorateProjectPricing
}) => {
  app.get('/api/projects', requireApiUserAuth, (req, res) => {
    const sessionUser = buildApiSessionUser(req.apiUser);
    const projects = db.projects()
      .filter((project) => isProjectVisibleToUser({ project, sessionUser }))
      .map(decorateProjectPricing);
    res.json({ projects });
  });

  app.get('/api/projects/:id', requireApiUserAuth, (req, res) => {
    const projects = db.projects();
    const project = decorateProjectPricing(projects.find((item) => item.id === req.params.id));
    if (!project) return res.status(404).json({ error: 'Project not found' });

    const sessionUser = buildApiSessionUser(req.apiUser);
    if (!isProjectVisibleToUser({ project, sessionUser })) {
      return res.status(403).json({ error: 'Not allowed' });
    }

    res.json({ project });
  });
};

module.exports = { registerProjectApiRoutes };
