import AppsIcon from '@material-ui/icons/Apps';
import ErrorIcon from '@material-ui/icons/Error';
import GroupIcon from '@material-ui/icons/Group';
import GroupWorkIcon from '@material-ui/icons/GroupWork';
import HelpIcon from '@material-ui/icons/Help';
import SearchIcon from '@material-ui/icons/Search';
import SettingsIcon from '@material-ui/icons/Settings';
import VisibilityIcon from '@material-ui/icons/Visibility';
import classnames from 'classnames';
import React from 'react';
import { Link as RouterLink } from 'react-router-dom';
import { Typography } from '../Typography/Typography';
import './navigation.scss';

export type NavigationItem =
  | 'settings'
  | 'overview'
  | 'search'
  | 'commands'
  | 'queries'
  | 'users';
type NavigationProps = {
  active?: NavigationItem;
};

export const Navigation = (props: NavigationProps) => (
  <div className="navigation">
    <RouterLink to="/settings" className="navigation__link-wrapper">
      <div
        className={classnames('navigation__link', {
          'navigation__link--active': props.active === 'settings',
        })}
      >
        <div className="navigation__link-icon">
          <SettingsIcon fontSize="inherit" />
        </div>
        <Typography size="m">Settings</Typography>
      </div>
    </RouterLink>
    <RouterLink to="" className="navigation__link-wrapper">
      <div
        className={classnames('navigation__link', {
          'navigation__link--active': props.active === 'overview',
        })}
      >
        <div className="navigation__link-icon">
          <VisibilityIcon fontSize="inherit" />
        </div>
        <Typography size="m">Overview</Typography>
      </div>
    </RouterLink>
    <RouterLink to="" className="navigation__link-wrapper">
      <div
        className={classnames('navigation__link', {
          'navigation__link--active': props.active === 'search',
        })}
      >
        <div className="navigation__link-icon">
          <SearchIcon fontSize="inherit" />
        </div>
        <Typography size="m">Search</Typography>
      </div>
    </RouterLink>
    <RouterLink to="" className="navigation__link-wrapper">
      <div
        className={classnames('navigation__link', {
          'navigation__link--active': props.active === 'commands',
        })}
      >
        <div className="navigation__link-icon">
          <ErrorIcon fontSize="inherit" />
        </div>
        <Typography size="m">Commands</Typography>
      </div>
    </RouterLink>
    <RouterLink to="" className="navigation__link-wrapper">
      <div
        className={classnames('navigation__link', {
          'navigation__link--active': props.active === 'queries',
        })}
      >
        <div className="navigation__link-icon">
          <HelpIcon fontSize="inherit" />
        </div>
        <Typography size="m">Queries</Typography>
      </div>
    </RouterLink>
    <RouterLink to="" className="navigation__link-wrapper">
      <div
        className={classnames('navigation__link', {
          'navigation__link--active': props.active === 'queries',
        })}
      >
        <div className="navigation__link-icon">
          <AppsIcon fontSize="inherit" />
        </div>
        <Typography size="m">Apps</Typography>
      </div>
    </RouterLink>
    <RouterLink to="" className="navigation__link-wrapper">
      <div
        className={classnames('navigation__link', {
          'navigation__link--active': props.active === 'users',
        })}
      >
        <div className="navigation__link-icon">
          <GroupIcon fontSize="inherit" />
        </div>
        <Typography size="m">Users</Typography>
      </div>
    </RouterLink>
    <RouterLink to="" className="navigation__link-wrapper">
      <div
        className={classnames('navigation__link', {
          'navigation__link--active': props.active === 'queries',
        })}
      >
        <div className="navigation__link-icon">
          <GroupWorkIcon fontSize="inherit" />
        </div>
        <Typography size="m">Contexts</Typography>
      </div>
    </RouterLink>
  </div>
);
