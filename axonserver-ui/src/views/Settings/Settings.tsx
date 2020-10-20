import { Grid } from '@material-ui/core';
import CancelIcon from '@material-ui/icons/Cancel';
import CheckCircleIcon from '@material-ui/icons/CheckCircle';
import classnames from 'classnames';

import React, { useEffect, useState } from 'react';
import { Card } from '../../components/Card/Card';
import { FormControl } from '../../components/FormControl/FormControl';
import { InputLabel } from '../../components/InputLabel/InputLabel';
import { MenuItem } from '../../components/MenuItem/MenuItem';
import { Select } from '../../components/Select/Select';
import { SettingsActivityTable } from '../../components/SettingsActivityTable/SettingsActivityTable';
import { SettingsConfigurationTable } from '../../components/SettingsConfigurationTable/SettingsConfigurationTable';
import { SettingsNodeTable } from '../../components/SettingsNodeTable/SettingsNodeTable';
import { Typography } from '../../components/Typography/Typography';
import { getMe, GetMeResponse } from '../../services/me/me';
import { getPublic, GetPublicResponse } from '../../services/public/public';
import { getVersion, GetVersionResponse } from '../../services/version/version';
import {
  getVisibleContexts,
  GetVisibleContextsResponse,
} from '../../services/visibleContexts/visibleContexts';
import './settings.scss';

export const Settings = () => {
  const [versionData, setVersionData] = useState<GetVersionResponse>();
  const [meData, setMeData] = useState<GetMeResponse>();
  const [publicResponseData, setPublicData] = useState<GetPublicResponse>();
  const [visibleContexts, setVisibleContexts] = useState<
    GetVisibleContextsResponse
  >();

  const [selectedContext, setSelectedContext] = useState<string>();

  useEffect(() => {
    getVersion().then((response) => setVersionData(response));
    getMe().then((response) => setMeData(response));
    getPublic().then((response) => setPublicData(response));
    getVisibleContexts().then((response) => {
      setVisibleContexts(response);
      const contextToSelect = response?.find(
        (context) => !context.startsWith('_'),
      );
      setSelectedContext(contextToSelect);
    });
  }, []);

  // Consider adding a "Loading..." component here.
  if (!versionData || !meData || !publicResponseData || !visibleContexts) {
    return null;
  }

  return (
    <Grid container spacing={2}>
      <Grid item md={6}>
        <SSLStatus enabled={meData.ssl} />
      </Grid>
      <Grid item md={6}>
        <AuthStatus enabled={meData.authentication} />
      </Grid>

      <Grid item md={6}>
        <SettingsConfigurationTable
          data={{
            name: meData.name,
            hostName: meData.hostName,
            httpPort: meData.httpPort,
            grpcPort: meData.grpcPort,
          }}
        />
      </Grid>
      <Grid item md={6}>
        <Card>
          <div className="settings__status-card-header">
            <Typography size="xxl" color="gray" weight="bold">
              Status
            </Typography>
            <div className="settings__select-context-wrapper">
              {selectedContext && (
                <FormControl fullWidth>
                  <InputLabel id="demo-simple-select-label">Context</InputLabel>
                  <Select
                    labelId="demo-simple-select-label"
                    id="demo-simple-select"
                    value={selectedContext}
                    onChange={(event) =>
                      setSelectedContext(event.target.value as string)
                    }
                  >
                    {visibleContexts.map((context, index) => (
                      <MenuItem key={`context${index}`} value={context}>
                        {context}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
              )}
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
};

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
