CREATE TABLE public."EventStream" (
	"Sequence"              bigserial              	NOT NULL,
	"AggregateRootTypeName" varchar(256)			NOT NULL,
	"AggregateRootId"       varchar(36) 			NOT NULL,
	"Version" 				int8 					NOT NULL,
	"CommandId" 			varchar(36) 			NOT NULL,
	"CreatedOn" 			timestamp 				NOT NULL,
	"Events" 				text 					NOT NULL,
	CONSTRAINT "PK_EventStream" PRIMARY KEY ("Sequence")
);
CREATE UNIQUE INDEX "IX_EventStream_AggId_Version" ON public."EventStream" ("AggregateRootId","Version");
CREATE UNIQUE INDEX "IX_EventStream_AggId_CommandId" ON public."EventStream" ("AggregateRootId","CommandId");
---------------------------

CREATE TABLE public."PublishedVersion" (
   	"Sequence"                bigserial             NOT NULL,
    "ProcessorName"           varchar (128)         NOT NULL,
    "AggregateRootTypeName"   varchar (256)         NOT NULL,
    "AggregateRootId"         varchar (36)          NOT NULL,
    "Version"                 int8                  NOT NULL,
    "CreatedOn"               timestamp             NOT NULL,
    "UpdatedOn"               timestamp             NOT NULL,
	CONSTRAINT "PK_PublishedVersion" PRIMARY KEY ("Sequence")
);
CREATE UNIQUE INDEX "IX_PublishedVersion_AggId_Version"   on public."PublishedVersion" ("ProcessorName" ASC,"AggregateRootId" ASC);

---------------------------------------
CREATE TABLE public."LockKey" (
    "Name"                   varchar (128)          NOT NULL,
    CONSTRAINT "PK_LockKey" PRIMARY KEY ("Name")
);
;