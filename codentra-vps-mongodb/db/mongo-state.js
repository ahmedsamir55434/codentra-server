const fs = require('fs');
const path = require('path');
const mongoose = require('mongoose');

mongoose.set('strictQuery', false);

const DEFAULT_LOYALTY_SETTINGS = {
  enabled: true,
  pointsPerEGP: 0.1,
  redeem: {
    enabled: true,
    pointsToEGP: 0.1,
    minPoints: 100
  }
};

const DATASET_CONFIG = {
  users: { file: 'users.json', defaultValue: () => [] },
  projects: { file: 'projects.json', defaultValue: () => [] },
  purchases: { file: 'purchases.json', defaultValue: () => [] },
  modifications: { file: 'modifications.json', defaultValue: () => [] },
  coupons: { file: 'coupons.json', defaultValue: () => [] },
  referrals: { file: 'referrals.json', defaultValue: () => [] },
  walletCodes: { file: 'wallet-codes.json', defaultValue: () => [] },
  appointments: { file: 'appointments.json', defaultValue: () => ({ timeSlots: [], bookings: [] }) },
  reviews: { file: 'reviews.json', defaultValue: () => [] },
  messages: { file: 'messages.json', defaultValue: () => [] },
  adminTeamMessages: { file: 'admin-team-messages.json', defaultValue: () => [] },
  carts: { file: 'carts.json', defaultValue: () => [] },
  invoices: { file: 'invoices.json', defaultValue: () => [] },
  meetingRecordings: { file: 'meeting-recordings.json', defaultValue: () => [] },
  subscriptionPlans: { file: 'subscription-plans.json', defaultValue: () => [] },
  subscriptions: { file: 'subscriptions.json', defaultValue: () => [] },
  subscriptionPayments: { file: 'subscription-payments.json', defaultValue: () => [] },
  subscriptionCoupons: { file: 'subscription-coupons.json', defaultValue: () => [] },
  loyaltySettings: { file: 'loyalty-settings.json', defaultValue: () => ({ ...DEFAULT_LOYALTY_SETTINGS }) }
};

const appStateSchema = new mongoose.Schema({
  key: {
    type: String,
    required: true,
    unique: true,
    index: true
  },
  data: {
    type: mongoose.Schema.Types.Mixed,
    required: true
  }
}, {
  collection: 'app_state',
  timestamps: true
});

const AppState = mongoose.models.AppState || mongoose.model('AppState', appStateSchema);

const clone = (value) => JSON.parse(JSON.stringify(value));

const ensureDirectory = (dirPath) => {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
};

const normalizeValue = (key, value) => {
  if (value === undefined) {
    return clone(DATASET_CONFIG[key].defaultValue());
  }

  if (key === 'appointments') {
    return {
      timeSlots: Array.isArray(value && value.timeSlots) ? value.timeSlots : [],
      bookings: Array.isArray(value && value.bookings) ? value.bookings : []
    };
  }

  if (key === 'loyaltySettings') {
    return {
      ...DEFAULT_LOYALTY_SETTINGS,
      ...(value || {}),
      redeem: {
        ...DEFAULT_LOYALTY_SETTINGS.redeem,
        ...((value && value.redeem) || {})
      }
    };
  }

  if (Array.isArray(DATASET_CONFIG[key].defaultValue())) {
    return Array.isArray(value) ? value : [];
  }

  return value;
};

const createMongoBackedDb = ({ dataDir }) => {
  ensureDirectory(dataDir);

  const state = {};
  let ready = false;
  let writeQueue = Promise.resolve();

  const readSeedData = (key) => {
    const config = DATASET_CONFIG[key];
    const filePath = path.join(dataDir, config.file);

    if (!fs.existsSync(filePath)) {
      const initial = normalizeValue(key, config.defaultValue());
      fs.writeFileSync(filePath, JSON.stringify(initial, null, 2));
      return initial;
    }

    try {
      const raw = fs.readFileSync(filePath, 'utf8');
      if (!raw.trim()) {
        return normalizeValue(key, config.defaultValue());
      }
      return normalizeValue(key, JSON.parse(raw));
    } catch (error) {
      console.warn(`Failed to parse seed data for ${key}, falling back to defaults.`, error.message);
      return normalizeValue(key, config.defaultValue());
    }
  };

  const writeSnapshot = (key, value) => {
    const config = DATASET_CONFIG[key];
    if (!config) return;

    const filePath = path.join(dataDir, config.file);
    ensureDirectory(path.dirname(filePath));
    fs.writeFileSync(filePath, JSON.stringify(value, null, 2));
  };

  const persistState = (key) => {
    const snapshot = clone(state[key]);
    writeQueue = writeQueue
      .then(async () => {
        await AppState.updateOne(
          { key },
          { $set: { data: snapshot } },
          { upsert: true }
        );
      })
      .catch((error) => {
        console.error(`Failed to persist ${key} to MongoDB:`, error);
      });

    return writeQueue;
  };

  const init = async ({ mongoUri }) => {
    await mongoose.connect(mongoUri, {
      serverSelectionTimeoutMS: 10000,
      family: 4
    });

    for (const key of Object.keys(DATASET_CONFIG)) {
      const existing = await AppState.findOne({ key }).lean();
      const initialValue = existing ? normalizeValue(key, existing.data) : readSeedData(key);

      state[key] = clone(initialValue);
      writeSnapshot(key, state[key]);

      if (!existing) {
        await AppState.updateOne(
          { key },
          { $set: { data: state[key] } },
          { upsert: true }
        );
      }
    }

    ready = true;
  };

  const db = {};

  for (const key of Object.keys(DATASET_CONFIG)) {
    const saveKey = `save${key.charAt(0).toUpperCase()}${key.slice(1)}`;

    db[key] = () => clone(state[key]);
    db[saveKey] = (value) => {
      state[key] = clone(normalizeValue(key, value));
      writeSnapshot(key, state[key]);
      void persistState(key);
    };
  }

  return {
    db,
    init,
    isReady: () => ready && mongoose.connection.readyState === 1,
    waitForWrites: async () => {
      await writeQueue;
    },
    disconnect: async () => {
      await writeQueue;
      if (mongoose.connection.readyState !== 0) {
        await mongoose.disconnect();
      }
      ready = false;
    }
  };
};

module.exports = {
  createMongoBackedDb
};
