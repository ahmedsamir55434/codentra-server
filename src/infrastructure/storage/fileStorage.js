const fs = require('fs');
const path = require('path');

const normalizeStoredPath = (storedPath) => {
  if (!storedPath) return null;
  if (typeof storedPath !== 'string') return null;
  return storedPath.startsWith('/') ? storedPath.slice(1) : storedPath;
};

const ensureDirectory = (dirPath) => {
  if (!fs.existsSync(dirPath)) fs.mkdirSync(dirPath, { recursive: true });
};

const copyFileIfMissing = (sourcePath, targetPath) => {
  if (!sourcePath || !targetPath) return;
  if (!fs.existsSync(sourcePath) || fs.existsSync(targetPath)) return;
  ensureDirectory(path.dirname(targetPath));
  fs.copyFileSync(sourcePath, targetPath);
};

const createStoredPathResolver = ({
  appRootDir,
  uploadsDir,
  bundledUploadsDir,
  privateUploadsDir,
  bundledPrivateUploadsDir,
  isVercel
}) => (storedPath) => {
  const normalized = normalizeStoredPath(storedPath);
  if (!normalized) return null;

  const candidates = [];

  if (normalized.startsWith('uploads/')) {
    const relativeUploadPath = normalized.slice('uploads/'.length);
    candidates.push(path.join(uploadsDir, relativeUploadPath));
    if (isVercel) candidates.push(path.join(bundledUploadsDir, relativeUploadPath));
  } else if (normalized.startsWith('private_uploads/')) {
    const relativePrivatePath = normalized.slice('private_uploads/'.length);
    candidates.push(path.join(privateUploadsDir, relativePrivatePath));
    if (isVercel) candidates.push(path.join(bundledPrivateUploadsDir, relativePrivatePath));
  } else {
    candidates.push(path.join(appRootDir, normalized));
  }

  for (const candidate of candidates) {
    if (candidate && fs.existsSync(candidate)) return candidate;
  }

  return candidates[0] || path.join(appRootDir, normalized);
};

const prepareRuntimeDirectories = ({ runtimePaths, isVercel }) => {
  [
    runtimePaths.dataDir,
    runtimePaths.uploadsDir,
    runtimePaths.meetingRecordingsDir,
    runtimePaths.adminTeamUploadsDir,
    runtimePaths.supportAttachmentsDir,
    runtimePaths.privateUploadsDir,
    runtimePaths.communityMediaDir,
    runtimePaths.communityCvsDir
  ].forEach(ensureDirectory);

  if (!isVercel || !fs.existsSync(runtimePaths.bundledDataDir)) return;

  fs.readdirSync(runtimePaths.bundledDataDir).forEach((entry) => {
    const sourcePath = path.join(runtimePaths.bundledDataDir, entry);
    if (!fs.statSync(sourcePath).isFile()) return;
    copyFileIfMissing(sourcePath, path.join(runtimePaths.dataDir, entry));
  });
};

module.exports = {
  normalizeStoredPath,
  ensureDirectory,
  createStoredPathResolver,
  prepareRuntimeDirectories
};
