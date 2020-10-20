import fetchMock from 'fetch-mock';
import { GetStatusResponse, statusUrl } from './status';

export async function mockGetStatus() {
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
    nrOfEvents: 7,
  };

  fetchMock.mock({
    url: statusUrl,
    method: 'GET',
    query: { context: 'default' },
    response: getStatusDefaultMock,
  });
  // .mock({url: statusUrl, method: 'GET', query: {context: 'billing'}, response: getStatusBillingMock});
  // .get(statusUrl, getStatusDefaultMock, {query: {context: 'default'}})
  // .get(statusUrl, getStatusBillingMock, {query: {context: 'billing'}});
}
