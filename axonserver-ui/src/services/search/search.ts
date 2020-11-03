export const searchUrl = '/v1/search';

export type SearchRowDataItem = {
  idValues: number[];
  sortValues: number[];
  value: {
    [key: string]: any;
  };
};

export const getSearchEventSource = (queryData: {
  query: string;
  activeContext: string;
  timeConstraint: string;
  liveUpdates: boolean;
  forceReadFromLeader: boolean;
  clientToken: string;
}) => {
  const searchParams = new URLSearchParams();
  searchParams.set('query', queryData.query);
  searchParams.set('context', queryData.activeContext);
  searchParams.set('timewindow', queryData.timeConstraint);
  searchParams.set('liveupdates', queryData.liveUpdates.toString());
  searchParams.set('forceleader', queryData.forceReadFromLeader.toString());
  searchParams.set('clientToken', queryData.clientToken);
  return new EventSource(`${searchUrl}?${searchParams.toString()}`);
};
