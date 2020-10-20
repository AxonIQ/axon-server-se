import fetchMock from 'fetch-mock';
import { GetPublicResponse, publicUrl } from './public';

export function mockGetPublic() {
  const getPublicResponseMock: GetPublicResponse = [
    {
      internalHostName: 'axonserver-enterprise-1',
      httpPort: 8024,
      grpcInternalPort: 8224,
      grpcPort: 8124,
      connected: true,
      name: 'axonserver-enterprise-1',
      hostName: 'axonserver-enterprise-1',
    },
    {
      internalHostName: 'axonserver-enterprise-2',
      httpPort: 8025,
      grpcInternalPort: 8224,
      grpcPort: 8125,
      connected: true,
      name: 'axonserver-enterprise-2',
      hostName: 'axonserver-enterprise-2',
    },
    {
      internalHostName: 'axonserver-enterprise-3',
      httpPort: 8026,
      grpcInternalPort: 8224,
      grpcPort: 8126,
      connected: true,
      name: 'axonserver-enterprise-3',
      hostName: 'axonserver-enterprise-3',
    },
  ];

  fetchMock.config.overwriteRoutes = true;
  fetchMock.get(publicUrl, getPublicResponseMock);
}
