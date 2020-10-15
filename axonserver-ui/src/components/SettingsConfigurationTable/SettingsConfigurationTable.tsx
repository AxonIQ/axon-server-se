import React from 'react';
import { Typography } from '../Typography/Typography';
import { Table } from '../Table/Table';
import { TableBody } from '../TableBody/TableBody';
import { TableCell } from '../TableCell/TableCell';
import { TableHead } from '../TableHead/TableHead';
import { TableRow } from '../TableRow/TableRow';

export const SettingsConfigurationTable = () => (
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
            axonserver-enterprise-1
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
            axonserver-enterprise-1
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
            8024
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
            8124
          </Typography>
        </TableCell>
      </TableRow>
    </TableBody>
  </Table>
);
