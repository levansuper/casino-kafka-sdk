import { KafkaConfig, logLevel } from 'kafkajs';
import { Logger } from './logger';

export enum FinancialEvent {
  Transaction = 'financial-event.transaction',
}

export enum UserEvent {
  Login = 'user-event.login',
  Logout = 'user-event.logout',
  Register = 'user-event.register',
  Update = 'user-event.update',
  SessionExpired = 'user-event.session-expired',
  DataRequest = 'user-event.data-request',
  DataResponse = 'user-event.data-response',
  XpUpdate = 'user-event.xp-update',
  LevelUp = 'user-event.level-up',
  RewardUpdate = 'user-event.reward-update',
}

export enum ServerEvent {
  Crash = 'server-event.crash',
  HealthCheck = 'server-event.health-check',
  Restart = 'server-event.restart',
}

export type Topic = FinancialEvent | UserEvent | ServerEvent;

export const DEFAULT_CLIENT_ID = 'a1b2c3d4-e5f6-7890-abcd-ef1234567890';

// --- Financial event payloads ---

export interface TransactionData {
  casinoId: number;
  clientId: string;
  transactionId: string;
  userId: number;
  tokenCode: string;
  tokenAmount: string;
  fiatCode: string | null;
  fiatAmount: string | null;
  usdAmount: string | null;
  exchangeRate: string | null;
  type: string;
  balanceBefore: string;
  balanceAfter: string;
  status: string;
  referenceId: string | null;
  description: string | null;
  gameId: string | null;
  gameName: string | null;
  metadata: Record<string, unknown> | null;
  createdAt: string;
  completedAt: string | null;
}

// --- User event payloads ---

export interface LoginData {
  userId: string;
  ip: string;
  clientId: string;
}

export interface LogoutData {
  userId: string;
  clientId: string;
}

export interface RegisterData {
  userId: number;
  userGuid: string;
  email: string | null;
  username: string | null;
  signupMethod: string;
  ipAddress: string | null;
  referByUserId: number | null;
  createdAt: string;
  clientId: string;
}

export interface SessionExpiredData {
  userId: string;
  sessionId: string;
  clientId: string;
}

export interface UserDataRequestData {
  userIds: number[];
  requestId: string;
}

export interface UserDataResponseData {
  requestId: string;
  users: RegisterData[];
}

// --- XP & Level-up event payloads ---

export interface XpUpdateData {
  userId: number;
  clientId: string;
  xpGained: number;
  totalExperience: number;
  bonusExperience: number;
  tier: string;
  rating: number;
  timestamp: string;
}

export interface LevelUpData {
  userId: number;
  clientId: string;
  oldTier: string;
  oldRating: number;
  newTier: string;
  newRating: number;
  totalExperience: number;
  bonusExperience: number;
  rewardIds: number[];
  timestamp: string;
}

// --- Reward event payloads ---

export interface RewardItemData {
  id: number;
  name: string;
  rewardConfigId: number;
  rewardConfig: {
    name?: string;
    type: string;
    claimableRewardList: any[] | null;
    isAutoclaimable?: boolean;
    description?: string | null;
    imageUrl?: string | null;
    progressType?: string | null;
    progressTarget?: number | null;
  } | null;
  status: string;
  availableStartDate: string | null;
  availableEndDate: string | null;
  grantDate: string | null;
  claimStartDate: string | null;
  claimEndDate: string | null;
  claimDate: string | null;
  xpNeeded: number;
  xpAccumulated: number;
  progress: Record<string, any> | null;
  claimedRewardList: any[] | null;
  createdAt: string;
}

export interface RewardUpdateSignal {
  userId: number;
  clientId: string;
  reason: 'rakeback_accumulated' | 'reward_assigned' | 'reward_claimed' | 'recentplay_progress' | 'progress_updated' | 'progress_completed';
  reward: RewardItemData;
  timestamp: string;
}

// --- Server event payloads ---

export interface CrashData {
  serverId: string;
  error: string;
}

export interface HealthCheckData {
  serverId: string;
  status: string;
}

export interface RestartData {
  serverId: string;
  reason: string;
}

// --- Topic → Data type mapping ---

export interface TopicDataMap {
  [FinancialEvent.Transaction]: TransactionData;
  [UserEvent.Login]: LoginData;
  [UserEvent.Logout]: LogoutData;
  [UserEvent.Register]: RegisterData;
  [UserEvent.Update]: RegisterData;
  [UserEvent.SessionExpired]: SessionExpiredData;
  [UserEvent.DataRequest]: UserDataRequestData;
  [UserEvent.DataResponse]: UserDataResponseData;
  [UserEvent.XpUpdate]: XpUpdateData;
  [UserEvent.LevelUp]: LevelUpData;
  [UserEvent.RewardUpdate]: RewardUpdateSignal;
  [ServerEvent.Crash]: CrashData;
  [ServerEvent.HealthCheck]: HealthCheckData;
  [ServerEvent.Restart]: RestartData;
}

export interface KafkaClientConfig {
  brokers: string[];
  clientId: string;
  logLevel?: logLevel;
  logger?: Logger;
  /** Pass-through for any additional KafkaJS config */
  kafkaOptions?: Partial<KafkaConfig>;
}

export interface ProducerMessage<T = unknown> {
  key?: string;
  value: T;
  headers?: Record<string, string>;
}

export interface ConsumedMessage<T extends Topic = Topic> {
  topic: T;
  partition: number;
  offset: string;
  key: string | null;
  value: TopicDataMap[T];
  headers: Record<string, string | undefined>;
  timestamp: string;
}

export type MessageHandler<T extends Topic = Topic> = (message: ConsumedMessage<T>) => Promise<void> | void;

export interface Serializer<T = unknown> {
  serialize(data: T): Buffer;
  deserialize(buffer: Buffer): T;
}

export interface ProducerConfig {
  serializer?: Serializer;
  logger?: Logger;
}

export interface ConsumerConfig {
  groupId: string;
  serializer?: Serializer;
  /** Max partitions processed concurrently. Ignored when sequential is true. Default: 1 */
  concurrency?: number;
  /** Process messages strictly in order (one at a time). Default: true */
  sequential?: boolean;
  /** When true, handler errors propagate to KafkaJS so the offset is not committed. When false, errors are logged and swallowed. Default: true */
  propagateErrors?: boolean;
  logger?: Logger;
}

export interface SubscribeOptions {
  fromBeginning?: boolean;
}
