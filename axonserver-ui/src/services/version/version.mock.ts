import fetchMock from 'fetch-mock';
import { versionUrl, GetVersionResponse } from './version';

export function mockGetVersion() {
  const getVersionResponseMock: GetVersionResponse = {
    productName: 'Axon Server',
    version: '4.4.3',
  };

  fetchMock.config.overwriteRoutes = true;
  fetchMock.get(versionUrl, getVersionResponseMock);
}
