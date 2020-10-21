import { rest } from 'msw';
import { meUrl, GetMeResponse } from './me';

const getMeResponseMock: GetMeResponse = {
  authentication: false,
  clustered: false,
  ssl: false,
  adminNode: true,
  developmentMode: false,
  storageContextNames: ['default', 'billing'],
  contextNames: ['default', 'billing'],
  internalHostName: 'axonserver-enterprise-1',
  httpPort: 8024,
  grpcInternalPort: 0,
  grpcPort: 8124,
  name: 'axonserver-enterprise-1',
  hostName: 'axonserver-enterprise-1',
};

export const mockGetMe = rest.get(meUrl, (_, res, ctx) =>
  res(ctx.json(getMeResponseMock)),
);
