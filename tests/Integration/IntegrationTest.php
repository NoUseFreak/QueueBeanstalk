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

use Pheanstalk\Pheanstalk;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Queue\Executor\CallbackExecutor;
use Queue\Job\Job;
use Queue\Worker\Worker;

class IntegrationTest extends \PHPUnit_Framework_TestCase
{
    private static $callbackErrorMessage = 'Broken';

    protected function setUp()
    {
        $pheanstalk = new Pheanstalk(getenv('BEANSTALK_HOST'));

        if (!$pheanstalk->getConnection()->isServiceListening()) {
            $this->markTestSkipped('Beanstalk service is not available.');
        }
    }

    public static function jobCallback()
    {
        throw new \Exception(self::$callbackErrorMessage);
    }

    public function testAddJob()
    {
        $driver = new BeanstalkDriver(new Pheanstalk(getenv('BEANSTALK_HOST')));
        $driver->addJob(new Job('test', ['callback' => [self::class, 'jobCallback']]));

        $logger = $this->getMockBuilder(LoggerInterface::class)
            ->getMock();
        $logger->expects($this->atLeastOnce())
            ->method('log')
            ->will($this->returnCallback(function ($type, $data, $context = []) {
                if (LogLevel::ALERT == $type) {
                    $this->assertEquals(LogLevel::ALERT, $type);
                    $this->assertContains('error', $data);
                    $this->assertArrayHasKey('exception', $context);
                    $this->assertInstanceOf(\Exception::class, $context['exception']);
                    $this->assertContains(self::$callbackErrorMessage, $context['exception']->getMessage());
                }
            }));

        $worker = new Worker(new Queue($driver), new CallbackExecutor(), 1);
        $worker->setLogger($logger);
        $worker->run();
    }
}
