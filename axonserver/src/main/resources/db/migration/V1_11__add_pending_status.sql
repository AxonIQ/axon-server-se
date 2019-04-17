alter table context add (
    change_pending boolean,
    pending_since timestamp
  );

