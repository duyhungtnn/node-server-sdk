import { SegmentUsersCache } from '../cache/segementUsers';
import { GRPCClient } from '../grpc/client';
import { ProcessorEventsEmitter } from './processorEvents';
import { Cache } from '../cache/cache';
import { ApiId } from '../objects/apiId';
import { SegmentUsers } from '@bucketeer/node-evaluation';
import { createSchedule, removeSchedule } from '../schedule';

interface SegementUsersCacheProcessor {
  start(): void;
  stop(): void;
}

type SegementUsersCacheProcessorOptions = {
  cache: Cache;
  segmentUsersCache: SegmentUsersCache;
  pollingInterval: number;
  grpc: GRPCClient;
  eventEmitter: ProcessorEventsEmitter;
  cachePollingInterval: number;
  version: string;
  featureFlagTag: string;
};

const SEGEMENT_USERS_REQUESTED_AT = 'bucketeer_segment_users_requested_at';
const SEGEMENT_USERS_CACHE_TTL = 0;

function NewSegementUserCacheProcessor(
  options: SegementUsersCacheProcessorOptions,
): SegementUsersCacheProcessor {
  return new DefaultSegementUserCacheProcessor(options);
}

class DefaultSegementUserCacheProcessor implements SegementUsersCacheProcessor {
  private options: SegementUsersCacheProcessorOptions;
  private pollingScheduleID?: NodeJS.Timeout;

  constructor(options: SegementUsersCacheProcessorOptions) {
    this.options = options;
  }

  start() {
    const { pollingInterval } = this.options;
    this.pollingScheduleID = createSchedule(() => this.runUpdateCache, pollingInterval);
  }

  stop() {
    if (this.pollingScheduleID) removeSchedule(this.pollingScheduleID);
  }

  private internalError(error: any): Error {
    return new Error(`internal error while updating segment users: ${error}`);
  }

  private runUpdateCache() {
    try {
      this.updateCache();
    } catch (error) {
      this.pushErrorMetricsEvent(this.internalError(error));
    }
  }

  private async updateCache() {
    const { segmentUsersCache, grpc, version } = this.options;
    const requestedAt = await this.getSegmentUsersRequestedAt();
    const segmentIds = await segmentUsersCache.getIds();
    const startTime: number = Date.now();

    const resp = await grpc.getSegmentUsers(segmentIds, requestedAt, version);

    const latency = (Date.now() - startTime) / 1000;

    this.pushLatencyMetricsEvent(latency);
    this.pushSizeMetricsEvent(resp.serializeBinary().length);

    if (resp.getForceUpdate()) {
      this.deleteAllAndSaveLocalCache(requestedAt, resp.getSegmentUsersList());
    } else {
      this.updateLocalCache(
        requestedAt,
        resp.getSegmentUsersList(),
        resp.getDeletedSegmentIdsList(),
      );
    }
  }

  async deleteAllAndSaveLocalCache(requestedAt: number, segmentUsersList: SegmentUsers[]) {
    const { segmentUsersCache } = this.options;
    await segmentUsersCache.deleteAll();
    await this.updateLocalCache(requestedAt, segmentUsersList, []);
  }

  async updateLocalCache(
    requestedAt: number,
    segmentUsersList: SegmentUsers[],
    deletedSegmentIds: string[],
  ) {
    const { segmentUsersCache } = this.options;
    deletedSegmentIds.forEach(async (deletedSegmentId) => {
      await segmentUsersCache.delete(deletedSegmentId);
    });
    segmentUsersList.forEach(async (segmentUsers) => {
      await segmentUsersCache.put(segmentUsers);
    });
    await this.putSegmentUsersRequestedAt(requestedAt);
  }

  private async getSegmentUsersRequestedAt(): Promise<number> {
    const { cache } = this.options;
    const requestedAt = await cache.get(SEGEMENT_USERS_REQUESTED_AT);
    return requestedAt ? Number(requestedAt) : 0;
  }

  putSegmentUsersRequestedAt(requestedAt: number): Promise<void> {
    const { cache } = this.options;
    return cache.put(SEGEMENT_USERS_REQUESTED_AT, requestedAt, SEGEMENT_USERS_CACHE_TTL);
  }

  async pushLatencyMetricsEvent(latency: number) {
    const { eventEmitter } = this.options;
    eventEmitter.emit('pushLatencyMetricsEvent', {
      latency: latency,
      apiId: ApiId.GET_SEGMENT_USERS,
    });
  }

  async pushErrorMetricsEvent(error: any) {
    const { eventEmitter } = this.options;
    eventEmitter.emit('error', { error: error, apiId: ApiId.GET_SEGMENT_USERS });
  }

  async pushSizeMetricsEvent(size: number) {
    const { eventEmitter } = this.options;
    eventEmitter.emit('pushSizeMetricsEvent', { size: size, apiId: ApiId.GET_SEGMENT_USERS });
  }
}
