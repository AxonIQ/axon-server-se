import Tooltip from '@material-ui/core/Tooltip';
import AccessTimeIcon from '@material-ui/icons/AccessTime';
import GroupWorkIcon from '@material-ui/icons/GroupWork';
import HelpOutlineIcon from '@material-ui/icons/HelpOutline';
import React, { useState } from 'react';
import { FormControl } from '../../components/FormControl/FormControl';
import { MenuItem } from '../../components/MenuItem/MenuItem';
import { Select } from '../../components/Select/Select';
import { Checkbox } from '../../components/Checkbox/Checkbox';
import { SearchInput } from '../../components/SearchInput/SearchInput';
import './search.scss';
import { Button } from '../../components/Button/Button';
import Modal from '@material-ui/core/Modal';

export const Search = () => {
  const queryTimes = [
    'Last hour',
    'Last 2 hours',
    'Last day',
    'Last week',
    'Custom',
  ];
  const contexts = ['default', 'billing'];

  const [queryTimeWindow, setQueryTimeWindow] = useState(queryTimes[0]);
  const [activeContext, setActiveContext] = useState(contexts[0]);
  const [liveUpdates, setLiveUpdates] = useState(true);
  const [readFromLeader, setReadFromLeader] = useState(false);
  const [showHelp, setShowHelp] = useState(false);

  return (
    <div className="search__query-wrapper">
      <SearchInput
        multiline
        placeholder={'Please enter your query'}
        onSubmit={(value) => alert(value)}
      />

      <hr className="search__query-options-divider" />

      <div className="search__query-options-wrapper">
        <div className="search__query-options-item">
          <Checkbox
            checked={liveUpdates}
            onChange={(_, checked) => setLiveUpdates(checked)}
            label="Live Updates"
          />
        </div>
        <div className="search__query-options-item">
          <Checkbox
            checked={readFromLeader}
            onChange={(_, checked) => setReadFromLeader(checked)}
            label="Read From Leader"
          />
        </div>
        <div className="search__query-options-select-wrapper">
          <Tooltip title="Query time window">
            <div className="search__query-options-select-icon">
              <AccessTimeIcon />
            </div>
          </Tooltip>

          <FormControl className="search__query-options-item search__query-options-select">
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
        <div className="search__query-options-select-wrapper">
          <Tooltip title="Active context">
            <div className="search__query-options-select-icon">
              <GroupWorkIcon />
            </div>
          </Tooltip>

          <FormControl className="search__query-options-item search__query-options-select">
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
        <Tooltip title="About the query language">
          <div className="search__query-options-help">
            <Button onClick={() => setShowHelp(true)}>
              <HelpOutlineIcon />
            </Button>
          </div>
        </Tooltip>
      </div>
      <Modal
        open={showHelp}
        onClose={() => setShowHelp(false)}
        aria-labelledby="simple-modal-title"
        aria-describedby="simple-modal-description"
      >
        <div>Hello from help!</div>
      </Modal>
    </div>
  );
};
