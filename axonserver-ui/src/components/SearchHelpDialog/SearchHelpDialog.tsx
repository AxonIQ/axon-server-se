import React from 'react';
import { Dialog } from '../../components/Dialog/Dialog';
import { DialogContent } from '../../components/DialogContent/DialogContent';
import { DialogTitle } from '../../components/DialogTitle/DialogTitle';
import { Divider } from '../../components/Divider/Divider';
import { Link } from '../../components/Link/Link';
import { Typography } from '../../components/Typography/Typography';

type SearchHelpDialogProps = {
  open: boolean;
  onClose: () => void;
};
export const SearchHelpDialog = (props: SearchHelpDialogProps) => (
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
