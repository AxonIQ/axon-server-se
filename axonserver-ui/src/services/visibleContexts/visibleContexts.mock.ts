import fetchMock from 'fetch-mock';
import {
  GetVisibleContextsResponse,
  visibleContextsUrl,
} from './visibleContexts';

export function mockGetVisibleContexts() {
  const getVisibleContextsResponseMock: GetVisibleContextsResponse = [
    'default',
    'billing',
  ];

  fetchMock.config.overwriteRoutes = true;
  fetchMock.get(visibleContextsUrl, getVisibleContextsResponseMock);
}
