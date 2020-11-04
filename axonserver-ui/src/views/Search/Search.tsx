import Grid from '@material-ui/core/Grid';
import Tooltip from '@material-ui/core/Tooltip';
import AccessTimeIcon from '@material-ui/icons/AccessTime';
import GroupWorkIcon from '@material-ui/icons/GroupWork';
import HelpOutlineIcon from '@material-ui/icons/HelpOutline';
import React, { useState } from 'react';
import { Checkbox } from '../../components/Checkbox/Checkbox';
import { Divider } from '../../components/Divider/Divider';
import { FormControl } from '../../components/FormControl/FormControl';
import { IconButton } from '../../components/IconButton/IconButton';
import { MenuItem } from '../../components/MenuItem/MenuItem';
import { SearchHelpDialog } from '../../components/SearchHelpDialog/SearchHelpDialog';
import { SearchInput } from '../../components/SearchInput/SearchInput';
import { SearchResultTable } from '../../components/SearchResultTable/SearchResultTable';
import { Select } from '../../components/Select/Select';
import {
  getSearchEventSource,
  SearchRowDataItem,
} from '../../services/search/search';
import './search.scss';

type SearchMetadataEvent = {
  type: 'metadata';
  data: string;
};
type SearchRowDataEvent = {
  type: 'row';
  data: string;
};
export const Search = () => {
  const queryTimes = [
    'Last hour',
    'Last 2 hours',
    'Last day',
    'Last week',
    'Custom',
  ];
  const contexts = ['default', 'billing'];
  const clientToken =
    Math.random().toString(36).substring(2) + new Date().getTime().toString(36);

  const [queryInput, setQueryInput] = useState('');
  const [queryTimeWindow, setQueryTimeWindow] = useState(queryTimes[0]);
  const [activeContext, setActiveContext] = useState(contexts[0]);
  const [liveUpdates, setLiveUpdates] = useState(true);
  const [readFromLeader, setReadFromLeader] = useState(false);
  const [showHelp, setShowHelp] = useState(false);

  const [
    searchEventStream,
    setSearchEventStream,
  ] = useState<EventSource | null>(null);
  const [metadata, setMetadata] = useState<string[]>([]);
  const [rowData, setRowData] = useState<SearchRowDataItem[]>([]);

  return (
    <Grid container spacing={2}>
      <Grid item md={12}>
        <div className="search__query-wrapper">
          <SearchInput
            value={queryInput}
            multiline
            placeholder={'Please enter your query'}
            onSubmit={(query) => {
              setQueryInput(query);

              // Ready states can be:
              // 0 - CONNECTING
              // 1 - OPEN
              // 2 - CLOSED
              if (
                searchEventStream &&
                searchEventStream.readyState !== EventSource.CLOSED
              ) {
                searchEventStream.close();
              }

              const evtSource = getSearchEventSource({
                query,
                activeContext,
                liveUpdates,
                clientToken,
                timeConstraint: queryTimeWindow,
                forceReadFromLeader: readFromLeader,
              });
              const onSearchStreamMetadata = (event: unknown) => {
                setMetadata(JSON.parse((event as SearchMetadataEvent).data));
              };

              const onSearchStreamRow = (event: unknown) => {
                const newRowItem = JSON.parse(
                  (event as SearchRowDataEvent).data,
                );
                setRowData((prevState) => [...prevState, newRowItem]);
              };

              const onSearchStreamError = (e: any) => {
                console.error(e);

                if (e && e.data) {
                  alert(e.data);
                }
                if (evtSource.readyState === EventSource.CLOSED) {
                  return;
                }
                onSearchStreamDone();
              };

              const onSearchStreamDone = () => {
                evtSource.removeEventListener(
                  'metadata',
                  onSearchStreamMetadata,
                );
                evtSource.removeEventListener('row', onSearchStreamRow);
                evtSource.removeEventListener('done', onSearchStreamDone);
                evtSource.removeEventListener('error', onSearchStreamError);
                evtSource.close();
              };
              evtSource.addEventListener('metadata', onSearchStreamMetadata);
              evtSource.addEventListener('row', onSearchStreamRow);
              evtSource.addEventListener('error', onSearchStreamError);
              evtSource.addEventListener('done', onSearchStreamDone);

              setSearchEventStream(evtSource);
            }}
          />

          <Divider />

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
                <IconButton onClick={() => setShowHelp(true)}>
                  <HelpOutlineIcon />
                </IconButton>
              </div>
            </Tooltip>

            <SearchHelpDialog
              open={showHelp}
              onClose={() => setShowHelp(false)}
            />
          </div>
        </div>
      </Grid>
      {metadata.length > 0 && rowData.length > 0 && (
        <Grid item xs={12}>
          <SearchResultTable headers={metadata} data={rowData} />
        </Grid>
      )}
    </Grid>
  );
};
