import { searchUrl } from './search';

export const mockSearch = () => {
  // Please make MockEvent globally available on the window object first.
  // It is currently in the .storybook folder, to be loaded up with the storybook stories.
  MockEvent({
    url: `${searchUrl}?query=test&context=default&timewindow=Last+hour&liveupdates=true&forceleader=false&clientToken=*`,
    responses: [
      {
        name: 'metadata',
        data: [
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
        ],
      },
      {
        name: 'row',
        data: {
          idValues: [88],
          sortValues: [88],
          value: {
            token: 88,
            eventIdentifier: '4e646b18-276c-4029-be56-d3a0e681df22',
            aggregateIdentifier: '6CCB5D6B-65',
            aggregateSequenceNumber: 0,
            aggregateType: 'GiftCard',
            payloadType: 'io.axoniq.demo.giftcard.api.IssuedEvt',
            payloadRevision: '',
            payloadData:
              '<io.axoniq.demo.giftcard.api.IssuedEvt><id>6CCB5D6B-65</id><amount>100</amount></io.axoniq.demo.giftcard.api.IssuedEvt>',
            timestamp: '2020-11-03T12:00:50.506Z',
            metaData:
              '{traceId=065dd789-f091-49fc-b32a-1aba24d5313e, correlationId=065dd789-f091-49fc-b32a-1aba24d5313e}',
          },
        },
      },
      {
        name: 'row',
        data: {
          idValues: [55],
          sortValues: [55],
          value: {
            token: 55,
            eventIdentifier: '0b983590-c006-4720-a4b8-5b704f0527a4',
            aggregateIdentifier: '544900F9-77',
            aggregateSequenceNumber: 0,
            aggregateType: 'GiftCard',
            payloadType: 'io.axoniq.demo.giftcard.api.IssuedEvt',
            payloadRevision: '',
            payloadData:
              '<io.axoniq.demo.giftcard.api.IssuedEvt><id>544900F9-77</id><amount>100</amount></io.axoniq.demo.giftcard.api.IssuedEvt>',
            timestamp: '2020-11-03T12:00:50.218Z',
            metaData:
              '{traceId=4b3b2dd2-ac0c-483c-88a0-1b44548b6263, correlationId=4b3b2dd2-ac0c-483c-88a0-1b44548b6263}',
          },
        },
      },
      {
        name: 'row',
        data: {
          idValues: [22],
          sortValues: [22],
          value: {
            token: 22,
            eventIdentifier: 'cba57dd5-a958-459a-887a-3a1c916349cb',
            aggregateIdentifier: 'CEDB13B3-B4',
            aggregateSequenceNumber: 0,
            aggregateType: 'GiftCard',
            payloadType: 'io.axoniq.demo.giftcard.api.IssuedEvt',
            payloadRevision: '',
            payloadData:
              '<io.axoniq.demo.giftcard.api.IssuedEvt><id>CEDB13B3-B4</id><amount>100</amount></io.axoniq.demo.giftcard.api.IssuedEvt>',
            timestamp: '2020-11-03T12:00:50.124Z',
            metaData:
              '{traceId=48a44ec2-6d9e-49d9-881d-5d343a07f78e, correlationId=48a44ec2-6d9e-49d9-881d-5d343a07f78e}',
          },
        },
      },
      {
        name: 'row',
        data: {
          idValues: [80],
          sortValues: [80],
          value: {
            token: 80,
            eventIdentifier: 'bb40ca3d-96e1-464b-ba8e-d5ef436f5549',
            aggregateIdentifier: '4DB4FEDF-54',
            aggregateSequenceNumber: 0,
            aggregateType: 'GiftCard',
            payloadType: 'io.axoniq.demo.giftcard.api.IssuedEvt',
            payloadRevision: '',
            payloadData:
              '<io.axoniq.demo.giftcard.api.IssuedEvt><id>4DB4FEDF-54</id><amount>100</amount></io.axoniq.demo.giftcard.api.IssuedEvt>',
            timestamp: '2020-11-03T12:00:50.422Z',
            metaData:
              '{traceId=fe9d9481-7c13-4696-a3d6-e4e4ec6e176a, correlationId=fe9d9481-7c13-4696-a3d6-e4e4ec6e176a}',
          },
        },
      },
      {
        name: 'row',
        data: {
          idValues: [47],
          sortValues: [47],
          value: {
            token: 47,
            eventIdentifier: '6979bed7-6839-4065-8103-eedcad8455f6',
            aggregateIdentifier: 'A889548B-3C',
            aggregateSequenceNumber: 0,
            aggregateType: 'GiftCard',
            payloadType: 'io.axoniq.demo.giftcard.api.IssuedEvt',
            payloadRevision: '',
            payloadData:
              '<io.axoniq.demo.giftcard.api.IssuedEvt><id>A889548B-3C</id><amount>100</amount></io.axoniq.demo.giftcard.api.IssuedEvt>',
            timestamp: '2020-11-03T12:00:50.211Z',
            metaData:
              '{traceId=3a04d5b9-13f6-4b3c-ac42-6643d0596aa8, correlationId=3a04d5b9-13f6-4b3c-ac42-6643d0596aa8}',
          },
        },
      },
      {
        name: 'row',
        data: {
          idValues: [14],
          sortValues: [14],
          value: {
            token: 14,
            eventIdentifier: '2e8ab2f2-d817-4748-97bd-20a33ca730bf',
            aggregateIdentifier: 'B016B0B4-44',
            aggregateSequenceNumber: 0,
            aggregateType: 'GiftCard',
            payloadType: 'io.axoniq.demo.giftcard.api.IssuedEvt',
            payloadRevision: '',
            payloadData:
              '<io.axoniq.demo.giftcard.api.IssuedEvt><id>B016B0B4-44</id><amount>100</amount></io.axoniq.demo.giftcard.api.IssuedEvt>',
            timestamp: '2020-11-03T12:00:50.111Z',
            metaData:
              '{traceId=f3d97719-98bf-4a9f-bd60-c5c4a7c363fa, correlationId=f3d97719-98bf-4a9f-bd60-c5c4a7c363fa}',
          },
        },
      },
      {
        name: 'row',
        data: {
          idValues: [72],
          sortValues: [72],
          value: {
            token: 72,
            eventIdentifier: '0c882883-81dd-453d-9da8-a2c7dfd692dc',
            aggregateIdentifier: '0538E46C-FE',
            aggregateSequenceNumber: 0,
            aggregateType: 'GiftCard',
            payloadType: 'io.axoniq.demo.giftcard.api.IssuedEvt',
            payloadRevision: '',
            payloadData:
              '<io.axoniq.demo.giftcard.api.IssuedEvt><id>0538E46C-FE</id><amount>100</amount></io.axoniq.demo.giftcard.api.IssuedEvt>',
            timestamp: '2020-11-03T12:00:50.414Z',
            metaData:
              '{traceId=11152ca5-93aa-46cb-aec7-eaa8b7255741, correlationId=11152ca5-93aa-46cb-aec7-eaa8b7255741}',
          },
        },
      },
      {
        name: 'row',
        data: {
          idValues: [39],
          sortValues: [39],
          value: {
            token: 39,
            eventIdentifier: '6461d0be-5876-4810-9fa5-4fc3a3b524c2',
            aggregateIdentifier: 'A2024971-6C',
            aggregateSequenceNumber: 0,
            aggregateType: 'GiftCard',
            payloadType: 'io.axoniq.demo.giftcard.api.IssuedEvt',
            payloadRevision: '',
            payloadData:
              '<io.axoniq.demo.giftcard.api.IssuedEvt><id>A2024971-6C</id><amount>100</amount></io.axoniq.demo.giftcard.api.IssuedEvt>',
            timestamp: '2020-11-03T12:00:50.138Z',
            metaData:
              '{traceId=c8078468-321d-4ffa-ac48-f0726562b5cf, correlationId=c8078468-321d-4ffa-ac48-f0726562b5cf}',
          },
        },
      },
      {
        name: 'row',
        data: {
          idValues: [6],
          sortValues: [6],
          value: {
            token: 6,
            eventIdentifier: '9b9e63ad-91d3-46af-b3b0-2e99f2c49db1',
            aggregateIdentifier: '782773E3-05',
            aggregateSequenceNumber: 0,
            aggregateType: 'GiftCard',
            payloadType: 'io.axoniq.demo.giftcard.api.IssuedEvt',
            payloadRevision: '',
            payloadData:
              '<io.axoniq.demo.giftcard.api.IssuedEvt><id>782773E3-05</id><amount>100</amount></io.axoniq.demo.giftcard.api.IssuedEvt>',
            timestamp: '2020-11-03T12:00:49.877Z',
            metaData:
              '{traceId=d0485148-6ec8-40ca-a5fe-e8de1f999d1f, correlationId=d0485148-6ec8-40ca-a5fe-e8de1f999d1f}',
          },
        },
      },
      {
        name: 'done',
        data: 'Done',
      },
    ],
  });
};
