import { rest } from 'msw';
import { GetStatusResponse, statusUrl } from './status';

const getStatusDefaultMock: GetStatusResponse = {
  commandRate: {
    count: 0,
    oneMinuteRate: 0,
    fiveMinuteRate: 0,
    fifteenMinuteRate: 0,
  },
  queryRate: {
    count: 0,
    oneMinuteRate: 0,
    fiveMinuteRate: 0,
    fifteenMinuteRate: 0,
  },
  eventRate: {
    count: 0,
    oneMinuteRate: 0,
    fiveMinuteRate: 0,
    fifteenMinuteRate: 0,
  },
  snapshotRate: {
    count: 0,
    oneMinuteRate: 0,
    fiveMinuteRate: 0,
    fifteenMinuteRate: 0,
  },
  nrOfEvents: 6,
  nrOfSnapshots: 0,
  nrOfSubscriptionQueries: 0,
  nrOfActiveSubscriptionQueries: 0,
  nrOfSubscriptionQueriesUpdates: 0,
  eventTrackers: {},
};
const getStatusBillingMock: GetStatusResponse = {
  ...getStatusDefaultMock,
  commandRate: {
    count: 0,
    oneMinuteRate: 1,
    fiveMinuteRate: 0,
    fifteenMinuteRate: 0,
  },
  queryRate: {
    count: 0,
    oneMinuteRate: 1,
    fiveMinuteRate: 0,
    fifteenMinuteRate: 0,
  },
  eventRate: {
    count: 0,
    oneMinuteRate: 1,
    fiveMinuteRate: 0,
    fifteenMinuteRate: 0,
  },
  snapshotRate: {
    count: 0,
    oneMinuteRate: 1,
    fiveMinuteRate: 0,
    fifteenMinuteRate: 0,
  },
  nrOfEvents: 7,
};
export const mockGetStatus = rest.get(statusUrl, (req, res, ctx) => {
  switch (req.url.searchParams.get('context')) {
    case 'default':
      return res(ctx.json(getStatusDefaultMock));
    case 'billing':
      return res(ctx.json(getStatusBillingMock));
  }
});
