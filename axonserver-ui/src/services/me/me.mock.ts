import fetchMock from 'fetch-mock';
import { meUrl, GetMeResponse } from './me';

export function mockGetMe() {
  const getMeResponseMock: GetMeResponse = {
    authentication: false,
    clustered: false,
    ssl: false,
    adminNode: true,
    developmentMode: false,
    storageContextNames: ['default'],
    contextNames: ['default'],
    name: 'pop-os',
    hostName: 'pop-os',
    httpPort: 8024,
    internalHostName: null,
    grpcInternalPort: 0,
    grpcPort: 8124,
  };

  fetchMock.config.overwriteRoutes = true;
  fetchMock.get(meUrl, getMeResponseMock);
}
