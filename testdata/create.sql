create table if not exists `person` (
  `id` int unsigned,
  `first_name` varchar(255) not null,
  `last_name` varchar(255) not null,
  primary key(`id`)
);
create table if not exists `book` (
  `id` int unsigned,
  `name` varchar(255) not null,
  `content` text not null,
  primary key(`id`)
);
