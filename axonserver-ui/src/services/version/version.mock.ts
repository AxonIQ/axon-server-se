import { rest } from 'msw';
import { versionUrl, GetVersionResponse } from './version';

const getVersionResponseMock: GetVersionResponse = {
  productName: 'Axon Server',
  version: '4.4.3',
};
export const mockGetVersion = rest.get(versionUrl, (_, res, ctx) =>
  res(ctx.json(getVersionResponseMock)),
);
