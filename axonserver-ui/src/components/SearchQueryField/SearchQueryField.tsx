import { Tooltip } from '@material-ui/core';
import InputBase from '@material-ui/core/InputBase';
import AccessTimeIcon from '@material-ui/icons/AccessTime';
import GroupWorkIcon from '@material-ui/icons/GroupWork';
import SearchIcon from '@material-ui/icons/Search';
import React, { useState } from 'react';
import { FormControl } from '../../components/FormControl/FormControl';
import { MenuItem } from '../../components/MenuItem/MenuItem';
import { Select } from '../../components/Select/Select';
import { Checkbox } from '../Checkbox/Checkbox';
import { SearchInput } from '../SearchInput/SearchInput';
import './search-query-field.scss';

type SearchQueryFieldProps = {
  multiline?: boolean;
  placeholder?: string;
  onSubmit?: (value: string) => void;
};
export const SearchQueryField = (props: SearchQueryFieldProps) => {
  const queryTimes = [
    'Last hour',
    'Last 2 hours',
    'Last day',
    'Last week',
    'Custom',
  ];
  const contexts = ['default', 'billing'];

  const [searchQuery, setSearchQuery] = useState('');
  const [queryTimeWindow, setQueryTimeWindow] = useState(queryTimes[0]);
  const [activeContext, setActiveContext] = useState(contexts[0]);
  const [liveUpdates, setLiveUpdates] = useState(true);
  const [readFromLeader, setReadFromLeader] = useState(false);

  return (
    <div className="search-query-field__wrapper">
      <SearchInput
        multiline
        placeholder={'Please enter your query'}
        onSubmit={(value) => alert(value)}
      />
      <hr className="search-query-field__options-divider" />
      <div className="search-query-field__options-wrapper">
        <div className="search-query-field__options-item">
          <Checkbox
            checked={liveUpdates}
            onChange={(_, checked) => setLiveUpdates(checked)}
            label="Live Updates"
          />
        </div>
        <div className="search-query-field__options-item">
          <Checkbox
            checked={readFromLeader}
            onChange={(_, checked) => setReadFromLeader(checked)}
            label="Read From Leader"
          />
        </div>
        <div className="search-query-field__options-select-wrapper">
          <Tooltip title="Query time window">
            <div className="search-query-field__options-select-icon">
              <AccessTimeIcon />
            </div>
          </Tooltip>
          <FormControl className="search-query-field__options-item search-query-field__options-select">
            <Select
              labelId="query-time-window"
              id="demo-simple-select"
              displayEmpty
              value={queryTimeWindow}
              onChange={(event) => {
                setQueryTimeWindow(event.target.value as string);
              }}
            >
              {queryTimes.map((queryTime, index) => (
                <MenuItem key={`query-time-${index}`} value={queryTime}>
                  {queryTime}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </div>
        <div className="search-query-field__options-select-wrapper">
          <Tooltip title="Active context">
            <div className="search-query-field__options-select-icon">
              <GroupWorkIcon />
            </div>
          </Tooltip>
          <FormControl className="search-query-field__options-item search-query-field__options-select">
            <Select
              labelId="active-context"
              id="demo-simple-select"
              displayEmpty
              value={activeContext}
              onChange={(event) => {
                setActiveContext(event.target.value as string);
              }}
            >
              {contexts.map((context, index) => (
                <MenuItem key={`context-${index}`} value={context}>
                  {context}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </div>
      </div>
    </div>
  );
};
