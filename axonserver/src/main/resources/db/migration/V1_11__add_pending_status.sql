alter table context add (
    change_pending boolean,
    pending_since timestamp
  );

alter table application add (
  change_pending boolean,
  pending_since timestamp
  );