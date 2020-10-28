import Grid from '@material-ui/core/Grid';
import React from 'react';
import { SearchResultTable } from './SearchResultTable';

export default {
  title: 'Components/SearchResultTable',
  component: SearchResultTable,
};
export const Default = () => (
  <Grid container>
    <Grid item md={12}>
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
