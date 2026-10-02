const path = require('path');

const buildRuntimePaths = ({ appRootDir, isVercel }) => {
  const bundledDataDir = path.join(appRootDir, 'data');
  const bundledUploadsDir = path.join(appRootDir, 'uploads');
  const bundledPrivateUploadsDir = path.join(appRootDir, 'private_uploads');
  const runtimeRootDir = isVercel ? path.join('/tmp', 'codentra-runtime') : appRootDir;
  const dataDir = isVercel ? path.join(runtimeRootDir, 'data') : bundledDataDir;
  const uploadsDir = isVercel ? path.join(runtimeRootDir, 'uploads') : bundledUploadsDir;
  const privateUploadsDir = isVercel ? path.join(runtimeRootDir, 'private_uploads') : bundledPrivateUploadsDir;

  return {
    appRootDir,
    runtimeRootDir,
    bundledDataDir,
    bundledUploadsDir,
    bundledPrivateUploadsDir,
    dataDir,
    uploadsDir,
    privateUploadsDir,
    meetingRecordingsDir: path.join(uploadsDir, 'meeting-recordings'),
    adminTeamUploadsDir: path.join(uploadsDir, 'admin-team'),
    supportAttachmentsDir: path.join(uploadsDir, 'support-attachments'),
    communityMediaDir: path.join(uploadsDir, 'community-media'),
    communityCvsDir: path.join(privateUploadsDir, 'community-cvs')
  };
};

module.exports = { buildRuntimePaths };
