import { fetchWrapper } from '../../fetchWrapper';

export const statusUrl = '/v1/public/status';

export type GetStatusResponse = {
  commandRate: {
    count: number;
    fifteenMinuteRate: number;
    fiveMinuteRate: number;
    name?: string;
    oneMinuteRate: number;
  };
  eventRate: {
    count: number;
    fifteenMinuteRate: number;
    fiveMinuteRate: number;
    name?: string;
    oneMinuteRate: number;
  };
  queryRate: {
    count: number;
    fifteenMinuteRate: number;
    fiveMinuteRate: number;
    name?: string;
    oneMinuteRate: number;
  };
  snapshotRate: {
    count: number;
    fifteenMinuteRate: number;
    fiveMinuteRate: number;
    name?: string;
    oneMinuteRate: number;
  };
  eventTrackers: {
    [key: string]: any;
  };
  nrOfActiveSubscriptionQueries: number;
  nrOfEvents: number;
  nrOfSnapshots: number;
  nrOfSubscriptionQueries: number;
  nrOfSubscriptionQueriesUpdates: number;
};
export async function getStatusForContext(
  contextQuery: string,
): Promise<GetStatusResponse> {
  const searchParams = new URLSearchParams();
  searchParams.set('context', contextQuery);

  const response = await fetchWrapper.get(
    `${statusUrl}?${searchParams.toString()}`,
  );
  return response.json();
}
