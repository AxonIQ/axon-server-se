import { Grid } from '@material-ui/core';
import CancelIcon from '@material-ui/icons/Cancel';
import CheckCircleIcon from '@material-ui/icons/CheckCircle';
import classnames from 'classnames';
import React from 'react';
import { SettingsConfigurationTable } from '../../components/SettingsConfigurationTable/SettingsConfigurationTable';
import { SettingsActivityTable } from '../../components/SettingsActivityTable/SettingsActivityTable';
import { SettingsNodeTable } from '../../components/SettingsNodeTable/SettingsNodeTable';

import { Card } from '../../components/Card/Card';
import { FormControl } from '../../components/FormControl/FormControl';
import { InputLabel } from '../../components/InputLabel/InputLabel';
import { MenuItem } from '../../components/MenuItem/MenuItem';
import { Select } from '../../components/Select/Select';
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
      <SettingsConfigurationTable />
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

        <SettingsActivityTable />
      </Card>
    </Grid>

    <Grid item md={12}>
      <SettingsNodeTable />
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
