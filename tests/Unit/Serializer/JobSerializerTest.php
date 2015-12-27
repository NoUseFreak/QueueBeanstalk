<?php

/**
 * This file is part of the Queue package.
 *
 * (c) Dries De Peuter <dries@nousefreak.be>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */
namespace Queue\Serializer;

use Queue\Job\Job;

class JobSerializerTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var JobSerializer
     */
    private $serializer;

    protected function setUp()
    {
        $this->serializer = new JobSerializer();
    }

    public function testSerialize()
    {
        $expected = '{"name":"test","data":[]}';
        $this->assertEquals($expected, $this->serializer->serialize(new Job('test')));
    }

    public function testUnserialize()
    {
        $input = '{"name":"test","data":[]}';
        $id = 1;

        $this->assertEquals(new Job('test', ['_beanstalk_id' => $id]), $this->serializer->unserialize(new \Pheanstalk\Job($id, $input)));
    }
}
