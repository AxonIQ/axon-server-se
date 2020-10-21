import React from 'react';
import { Typography } from '../Typography/Typography';
import { Table } from '../Table/Table';
import { TableBody } from '../TableBody/TableBody';
import { TableCell } from '../TableCell/TableCell';
import { TableHead } from '../TableHead/TableHead';
import { TableRow } from '../TableRow/TableRow';

type SettingsConfigurationTableProps = {
  data: {
    name: string;
    hostName: string;
    httpPort: number;
    grpcPort: number;
  };
};
export const SettingsConfigurationTable = (
  props: SettingsConfigurationTableProps,
) => (
  <Table flat>
    <TableHead>
      <TableCell colSpan={2}>
        <Typography size="m" color="white" weight="bold" uppercase>
          Configuration
        </Typography>
      </TableCell>
    </TableHead>

    <TableBody>
      <TableRow>
        <TableCell smallWidth>
          <Typography size="m" weight="bold" color="gray" uppercase>
            Node name
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            {props.data.name}
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell smallWidth>
          <Typography size="m" weight="bold" color="gray" uppercase>
            Host name
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            {props.data.hostName}
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell smallWidth>
          <Typography size="m" weight="bold" color="gray" uppercase>
            HTTP port
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            {props.data.httpPort}
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell smallWidth>
          <Typography size="m" weight="bold" color="gray" uppercase>
            GRPC port
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            {props.data.grpcPort}
          </Typography>
        </TableCell>
      </TableRow>
    </TableBody>
  </Table>
);
