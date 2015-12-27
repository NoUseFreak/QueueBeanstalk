<?php

/**
 * This file is part of the Queue package.
 *
 * (c) Dries De Peuter <dries@nousefreak.be>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
namespace Queue;

use Pheanstalk\PheanstalkInterface;
use Queue\Job\Job;

class BeanstalkDriverTest extends \PHPUnit_Framework_TestCase
{
    public function testAddJob()
    {
        $pheanstalk = $this->getMockBuilder(PheanstalkInterface::class)
            ->getMock();

        $pheanstalk->expects($this->once())
            ->method('put');

        $driver = new BeanstalkDriver($pheanstalk);

        $driver->addJob(new Job('test'));
    }

    public function testResolveJob()
    {
        $pheanstalk = $this->getMockBuilder(PheanstalkInterface::class)
            ->getMock();

        $pheanstalk->expects($this->once())
            ->method('reserve')
            ->willReturn(new \Pheanstalk\Job(1, '{"name": "test", "data": []}'));

        $driver = new BeanstalkDriver($pheanstalk);

        $driver->resolveJob();
    }
}
