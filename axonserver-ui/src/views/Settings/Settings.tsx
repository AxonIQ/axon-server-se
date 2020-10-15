import { Grid } from '@material-ui/core';
import CancelIcon from '@material-ui/icons/Cancel';
import CheckCircleIcon from '@material-ui/icons/CheckCircle';
import classnames from 'classnames';
import React from 'react';

import { Card } from '../../components/Card/Card';
import { FormControl } from '../../components/FormControl/FormControl';
import { InputLabel } from '../../components/InputLabel/InputLabel';
import { MenuItem } from '../../components/MenuItem/MenuItem';
import { Select } from '../../components/Select/Select';
import { Table } from '../../components/Table/Table';
import { TableBody } from '../../components/TableBody/TableBody';
import { TableCell } from '../../components/TableCell/TableCell';
import { TableHead } from '../../components/TableHead/TableHead';
import { TableRow } from '../../components/TableRow/TableRow';
import { Typography } from '../../components/Typography/Typography';
import './settings.scss';

export const Settings = () => (
  <Grid container spacing={2}>
    <Grid item md={6}>
      <SSLStatus enabled={false} />
    </Grid>
    <Grid item md={6}>
      <AuthStatus enabled={false} />
    </Grid>

    <Grid item md={6}>
      <ConfigurationTable />
    </Grid>
    <Grid item md={6}>
      <Card>
        <div className="settings__status-card-header">
          <Typography size="xxl" color="gray" weight="bold">
            Status
          </Typography>
          <div className="settings__select-context-wrapper">
            <FormControl fullWidth>
              <InputLabel id="demo-simple-select-label">Context</InputLabel>
              <Select
                labelId="demo-simple-select-label"
                id="demo-simple-select"
                // value={age}
                // onChange={handleChange}
              >
                <MenuItem value={10}>billing</MenuItem>
                <MenuItem value={20}>default</MenuItem>
                <MenuItem value={30}>testcontext@demo</MenuItem>
              </Select>
            </FormControl>
          </div>
        </div>
        <Typography size="l" color="light" addMargin>
          <div className="settings__last-event-token-wrapper">
            <div>Last Event Token:</div>
            <div>-1</div>
          </div>
        </Typography>

        <ActivityTable />
      </Card>
    </Grid>
    <Grid item md={12}>
      <NodeTable />
    </Grid>
  </Grid>
);

type SSLStatusProps = {
  enabled?: boolean;
};
const SSLStatus = (props: SSLStatusProps) => (
  <div
    className={classnames(
      'settings__status-card',
      `settings__status-card--${props.enabled ? 'enabled' : 'disabled'}`,
    )}
  >
    <Typography size="xl" weight="bold">
      SSL {props.enabled ? 'Enabled' : 'Disabled'}
    </Typography>
    <div className="settings__status-card-icon">
      {props.enabled ? (
        <CheckCircleIcon fontSize="inherit" />
      ) : (
        <CancelIcon fontSize="inherit" />
      )}
    </div>
  </div>
);

type AuthStatusProps = {
  enabled?: boolean;
};
const AuthStatus = (props: AuthStatusProps) => (
  <div
    className={classnames(
      'settings__status-card',
      `settings__status-card--${props.enabled ? 'enabled' : 'disabled'}`,
    )}
  >
    <Typography size="xl" weight="bold">
      Authentication {props.enabled ? 'Enabled' : 'Disabled'}
    </Typography>
    <div className="settings__status-card-icon">
      {props.enabled ? (
        <CheckCircleIcon fontSize="inherit" />
      ) : (
        <CancelIcon fontSize="inherit" />
      )}
    </div>
  </div>
);

const ConfigurationTable = () => (
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

const ActivityTable = () => (
  <Table flat>
    <TableHead>
      <TableCell>
        <Typography size="m" color="white" weight="bold" uppercase>
          Activity In The Last Minute
        </Typography>
      </TableCell>
      <TableCell align="right"></TableCell>
    </TableHead>

    <TableBody>
      <TableRow>
        <TableCell>
          <Typography size="m" color="gray" weight="bold" uppercase>
            Commands received / second
          </Typography>
        </TableCell>
        <TableCell align="right">
          <Typography size="m" color="gray">
            0
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell>
          <Typography size="m" color="gray" weight="bold" uppercase>
            Queries received / second
          </Typography>
        </TableCell>
        <TableCell align="right">
          <Typography size="m" color="gray">
            0
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell>
          <Typography size="m" color="gray" weight="bold" uppercase>
            Events stored / second
          </Typography>
        </TableCell>
        <TableCell align="right">
          <Typography size="m" color="gray">
            0
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell>
          <Typography size="m" color="gray" weight="bold" uppercase>
            Snapshots stored / second
          </Typography>
        </TableCell>
        <TableCell align="right">
          <Typography size="m" color="gray">
            0
          </Typography>
        </TableCell>
      </TableRow>
    </TableBody>
  </Table>
);

const NodeTable = () => (
  <Table flat>
    <TableHead>
      <TableCell>
        <Typography size="m" color="white" weight="bold" uppercase>
          Node Name
        </Typography>
      </TableCell>
      <TableCell>
        <Typography size="m" color="white" weight="bold" uppercase>
          Host Name
        </Typography>
      </TableCell>
      <TableCell>
        <Typography size="m" color="white" weight="bold" uppercase>
          HTTP Port
        </Typography>
      </TableCell>
      <TableCell>
        <Typography size="m" color="white" weight="bold" uppercase>
          GRPC Port
        </Typography>
      </TableCell>
    </TableHead>

    <TableBody>
      <TableRow>
        <TableCell>
          <Typography size="m" color="gray">
            axonserver-enterprise-1
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            axonserver-enterprise-1
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            8024
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            8124
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell>
          <Typography size="m" color="gray">
            axonserver-enterprise-2
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            axonserver-enterprise-2
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            8025
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            8125
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell>
          <Typography size="m" color="gray">
            axonserver-enterprise-3
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            axonserver-enterprise-3
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            8026
          </Typography>
        </TableCell>
        <TableCell>
          <Typography size="m" color="gray">
            8126
          </Typography>
        </TableCell>
      </TableRow>
    </TableBody>
  </Table>
);
