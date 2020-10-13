import { Container, Grid } from '@material-ui/core';
import React from 'react';
import { Card } from '../../components/Card/Card';
import { CardContent } from '../../components/CardContent/CardContent';
import { Typography } from '../../components/Typography/Typography';
import classnames from 'classnames';

import CancelIcon from '@material-ui/icons/Cancel';
import CheckCircleIcon from '@material-ui/icons/CheckCircle';

import './settings.scss';

export const Settings = () => (
  <Container maxWidth="xl">
    <Grid container spacing={2}>
      <Grid item md={6}>
        <SSLStatus enabled={false} />
      </Grid>
      <Grid item md={6}>
        <AuthStatus enabled={false} />
      </Grid>

      <Grid item md={6}>
        <Card>
          <CardContent>
            <Typography center size="l" weight="bold">
              Settings
            </Typography>
          </CardContent>
        </Card>
      </Grid>
      <Grid item md={6}>
        <Card>
          <CardContent>Hello from card</CardContent>
        </Card>
      </Grid>
    </Grid>
  </Container>
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
