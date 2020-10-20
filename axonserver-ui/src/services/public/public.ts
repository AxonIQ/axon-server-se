export const publicUrl = '/v1/public';

export type GetPublicResponse = Array<{
  connected: boolean;
  grpcInternalPort: number;
  grpcPort: number;
  hostName: string;
  httpPort: number;
  internalHostName: string;
  name: string;
}>;
export async function getPublic(): Promise<GetPublicResponse> {
  const response = await fetch(publicUrl);
  return response.json();
}
