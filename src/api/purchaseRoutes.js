const registerPurchaseDownloadApiRoutes = ({
  app,
  requireApiUserAuth,
  db,
  verifyToken,
  path,
  fs,
  appRootDir,
  isProjectDownloadsLocked,
  getProjectDownloadsLockReason,
  getDownloadFileName,
  recordDownloadEvent
}) => {
  app.get('/api/purchases/:id/download-url', requireApiUserAuth, (req, res) => {
    const purchase = db.purchases().find((item) => item && item.id === req.params.id && item.userId === req.apiUser.id);
    if (!purchase) return res.status(404).json({ error: 'Purchase not found' });
    if (purchase.status !== 'approved') return res.status(400).json({ error: 'الملف غير متاح للتحميل بعد' });
    if (purchase.downloadLocked) {
      return res.status(403).json({ error: purchase.downloadLockReason || 'تم قفل هذه النسخة من المشروع بواسطة الأدمن' });
    }

    if (purchase.projectId) {
      const project = db.projects().find((item) => item && item.id === purchase.projectId);
      if (project && isProjectDownloadsLocked(project)) {
        return res.status(403).json({ error: getProjectDownloadsLockReason(project) || 'تم قفل تنزيلات هذا المشروع مؤقتاً' });
      }
    }

    return res.json({
      url: `/api/download/${purchase.id}`,
      fileName: getDownloadFileName(purchase)
    });
  });

  app.get('/api/download/:purchaseId', (req, res) => {
    const token = String(req.query.token || '').trim();
    const decoded = token ? verifyToken(token) : null;
    if (!decoded || decoded.role !== 'user') {
      return res.status(401).json({ error: 'Invalid token' });
    }

    const purchase = db.purchases().find((item) => item && item.id === req.params.purchaseId && item.userId === decoded.id);
    if (!purchase) return res.status(404).json({ error: 'Purchase not found' });
    if (purchase.status !== 'approved') return res.status(400).json({ error: 'File not available' });
    if (purchase.downloadLocked) {
      return res.status(403).json({ error: purchase.downloadLockReason || 'Download locked' });
    }

    if (purchase.projectId) {
      const project = db.projects().find((item) => item && item.id === purchase.projectId);
      if (project && isProjectDownloadsLocked(project)) {
        return res.status(403).json({ error: getProjectDownloadsLockReason(project) || 'Project downloads are locked' });
      }
    }

    const absoluteFilePath = path.resolve(appRootDir, purchase.filePath);
    if (!fs.existsSync(absoluteFilePath)) {
      return res.status(404).json({ error: 'File not found' });
    }

    recordDownloadEvent({
      req,
      kind: 'purchase',
      userId: decoded.id,
      purchaseId: purchase.id,
      projectId: purchase.projectId || null,
      meta: { via: 'api-token' }
    });

    return res.download(absoluteFilePath, getDownloadFileName(purchase));
  });
};

module.exports = { registerPurchaseDownloadApiRoutes };
