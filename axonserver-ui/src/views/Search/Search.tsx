import Grid from '@material-ui/core/Grid';
import Tooltip from '@material-ui/core/Tooltip';
import AccessTimeIcon from '@material-ui/icons/AccessTime';
import GroupWorkIcon from '@material-ui/icons/GroupWork';
import HelpOutlineIcon from '@material-ui/icons/HelpOutline';
import React, { useState } from 'react';
import { Checkbox } from '../../components/Checkbox/Checkbox';
import { Dialog } from '../../components/Dialog/Dialog';
import { DialogContent } from '../../components/DialogContent/DialogContent';
import { DialogTitle } from '../../components/DialogTitle/DialogTitle';
import { Divider } from '../../components/Divider/Divider';
import { FormControl } from '../../components/FormControl/FormControl';
import { IconButton } from '../../components/IconButton/IconButton';
import { Link } from '../../components/Link/Link';
import { MenuItem } from '../../components/MenuItem/MenuItem';
import { SearchInput } from '../../components/SearchInput/SearchInput';
import { SearchResultTable } from '../../components/SearchResultTable/SearchResultTable';
import { Select } from '../../components/Select/Select';
import { Typography } from '../../components/Typography/Typography';
import './search.scss';

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

  // Instantiating an `EventSource`
  const evtSource = new EventSource('/tweets');

  // Listening to specific event names and handling them
  evtSource.addEventListener(
    'tweet',
    function (e) {
      console.log(e);
    },
    false,
  );

  return (
    <Grid container spacing={2}>
      <Grid item md={12}>
        <div className="search__query-wrapper">
          <SearchInput
            multiline
            placeholder={'Please enter your query'}
            onSubmit={(value) => alert(value)}
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

            <HelpDialog open={showHelp} onClose={() => setShowHelp(false)} />
          </div>
        </div>
      </Grid>
      <Grid item xs={12}>
        <SearchResultTable
          headers={[
            'token',
            'eventIdentifier',
            'aggregateIdentifier',
            'aggregateSequenceNumber',
            'aggregateType',
            'payloadType',
            'payloadRevision',
            'payloadData',
            'timestamp',
            'metaData',
          ]}
          data={[
            {
              idValues: [88],
              sortValues: [88],
              value: {
                aggregateSequenceNumber: 0,
                metaData:
                  '{traceId=ee57c132-1b38-4b51-a232-88a2b1b347a1, correlationId=ee57c132-1b38-4b51-a232-88a2b1b347a1}',
                payloadType: 'io.axoniq.demo.giftcard.api.IssuedEvt',
                payloadRevision: '',
                aggregateIdentifier: '2793B9E1-EB',
                payloadData:
                  '<io.axoniq.demo.giftcard.api.IssuedEvt><id>2793B9E1-EB</id><amount>100</amount></io.axoniq.demo.giftcard.api.IssuedEvt>',
                eventIdentifier: 'd074d0e2-2718-4f5a-a638-2c02649038ea',
                token: 88,
                aggregateType: 'GiftCard',
                timestamp: '2020-10-28T14:45:26.749Z',
              },
            },
            {
              idValues: [89],
              sortValues: [89],
              value: {
                aggregateSequenceNumber: 0,
                metaData:
                  '{traceId=ee57c132-1b38-4b51-a232-88a2b1b347a1, correlationId=ee57c132-1b38-4b51-a232-88a2b1b347a1}',
                payloadType: 'io.axoniq.demo.giftcard.api.IssuedEvt',
                payloadRevision: '',
                aggregateIdentifier: '2793B9E1-EB',
                payloadData:
                  '<io.axoniq.demo.giftcard.api.IssuedEvt><id>2793B9E1-EB</id><amount>100</amount></io.axoniq.demo.giftcard.api.IssuedEvt>',
                eventIdentifier: 'd074d0e2-2718-4f5a-a638-2c02649038ea',
                token: 89,
                aggregateType: 'GiftCard',
                timestamp: '2020-10-28T14:45:26.749Z',
              },
            },
            {
              idValues: [90],
              sortValues: [90],
              value: {
                aggregateSequenceNumber: 0,
                metaData:
                  '{traceId=ee57c132-1b38-4b51-a232-88a2b1b347a1, correlationId=ee57c132-1b38-4b51-a232-88a2b1b347a1}',
                payloadType: 'io.axoniq.demo.giftcard.api.IssuedEvt',
                payloadRevision: '',
                aggregateIdentifier: '2793B9E1-EB',
                payloadData:
                  '<io.axoniq.demo.giftcard.api.IssuedEvt><id>2793B9E1-EB</id><amount>100</amount></io.axoniq.demo.giftcard.api.IssuedEvt>',
                eventIdentifier: 'd074d0e2-2718-4f5a-a638-2c02649038ea',
                token: 90,
                aggregateType: 'GiftCard',
                timestamp: '2020-10-28T14:45:26.749Z',
              },
            },
          ]}
        />
      </Grid>
    </Grid>
  );
};

type HelpDialog = {
  open: boolean;
  onClose: () => void;
};
const HelpDialog = (props: HelpDialog) => (
  <Dialog
    fullWidth={true}
    maxWidth={'lg'}
    open={props.open}
    onClose={props.onClose}
  >
    <DialogTitle onClose={props.onClose}>About the query language</DialogTitle>
    <DialogContent dividers>
      <Typography size="m" addMargin>
        You can query the event store through this page. The query operates on a
        stream of events, where you can define filters and projections to obtain
        the results that you want.{' '}
      </Typography>
      <Typography size="m" addMargin>
        When you perform a search without any query it returns 1000 events.
      </Typography>
      <Typography size="m" addMargin>
        The event stream contains the following fields:
      </Typography>
      <Typography size="m" addMargin>
        <code>token</code>
        <br />
        <code>aggregateIdentifier</code>
        <br />
        <code>aggregateSequenceNumber</code>
        <br />
        <code>aggregateType</code>
        <br />
        <code>payloadType</code>
        <br />
        <code>payloadRevision</code>
        <br />
        <code>payloadData</code>
        <br />
        <code>timestamp</code>
        <br />
      </Typography>
      <Divider />

      <Typography size="l" weight="bold" addMargin>
        Filtering
      </Typography>
      <Typography size="m" addMargin>
        Filtering lets you reduce the events you see, so you get only those
        events that you want. A simple filter is to find all the events for a
        specific aggregate:
      </Typography>
      <Typography size="m" addMargin>
        <code>
          aggregateIdentifier = "beff70ef-3160-499b-8409-5bd5646f52f3"
        </code>
      </Typography>
      <Typography size="m" addMargin>
        You can also filter based on a partial value, for instance a string
        within the payloadData:
      </Typography>
      <Typography size="m" addMargin>
        <code>payloadData contains "ACME"</code>
      </Typography>
      <Typography size="m" addMargin>
        And, of course, you can combine these filters with AND or OR:
      </Typography>
      <Typography size="m" addMargin>
        <code>
          aggregateIdentifier = "beff70ef-3160-499b-8409-5bd5646f52f3" and
          payloadData contains "ACME"
        </code>
      </Typography>
      <Divider />

      <Typography size="l" weight="bold" addMargin>
        Projections
      </Typography>
      <Typography size="m" addMargin>
        You can select the fields, perform operations on the fields and perform
        grouping functions. For instance you can perform a query that returns
        the number of events grouped by the payloadType:
      </Typography>
      <Typography size="m" addMargin>
        <code>groupby(payloadType, count())</code>
      </Typography>
      <Typography size="m" addMargin>
        You can also select a number of fields and perform operations on them:
      </Typography>
      <Typography size="m" addMargin>
        <code>
          select(aggregateType, payloadType, formatDate(timestamp, "yyyy/MM/dd
          HH:MM") as time)
        </code>
      </Typography>
      <Typography size="m" addMargin>
        Combining filters and projections is also possible, by creating a
        pipeline of conditions, e.g.:
      </Typography>
      <Typography size="m" addMargin>
        <code>
          aggregateSequenceNumber {'>'} 50 | groupby(payloadType, count())
        </code>
      </Typography>
      <Divider />

      <Typography size="l" weight="bold" addMargin>
        Limiting results
      </Typography>
      <Typography size="m" addMargin>
        Filtering on recent events is done by adding a time constraint to the
        query chain. An example:
      </Typography>
      <Typography size="m" addMargin>
        <code>aggregateSequenceNumber {'>'} 50 | last 4 hours</code>
      </Typography>
      <Divider />

      <Typography size="l" weight="bold" addMargin>
        Functions
      </Typography>
      <Typography size="m" addMargin>
        For more information on all supported functions check{' '}
        <Link
          type="secondary"
          underline
          target="_blank"
          to="https://docs.axoniq.io/reference-guide/"
        >
          the reference guide.
        </Link>
      </Typography>
    </DialogContent>
  </Dialog>
);
