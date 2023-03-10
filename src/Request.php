<?php declare(strict_types=1);

/*
  Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License version 2 or any later
  version. You should have received a copy of the GPL license along with this
  program; if you did not, you can find it at http://www.gnu.org/
*/

namespace Manticoresearch\Buddy\Plugin\ShowQueries;

use Manticoresearch\Buddy\Core\Network\Request as NetworkRequest;
use Manticoresearch\Buddy\Core\Plugin\Request as BaseRequest;

/**
 * Request for Backup command that has parsed parameters from SQL
 */
final class Request extends BaseRequest {
	public string $query;
	public string $path;
	public bool $hasCliEndpoint;

	/**
	 * @param NetworkRequest $request
	 * @return static
	 */
	public static function fromNetworkRequest(NetworkRequest $request): static {
		$self = new static();
		$self->query = 'SELECT * FROM @@system.sessions';
		[$self->path, $self->hasCliEndpoint] = self::getEndpointInfo($request);
		return $self;
	}

	/**
	 * @param NetworkRequest $request
	 * @return bool
	 */
	public static function hasMatch(NetworkRequest $request): bool {
		return strlen($request->payload) === 12 && strncasecmp($request->payload, 'show queries', 12) === 0;
	}
}
