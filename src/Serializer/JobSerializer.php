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
use Queue\Job\JobInterface;

class JobSerializer
{
    public function serialize(JobInterface $job)
    {
        return json_encode([
            'name' => $job->getName(),
            'data' => $job->getData(),
        ]);
    }

    public function unserialize(\Pheanstalk\Job $pheanstalkJob)
    {
        $data = json_decode($pheanstalkJob->getData());
        $data->data = (array) $data->data;
        $data->data['_beanstalk_id'] = $pheanstalkJob->getId();

        return new Job($data->name, $data->data);
    }
}
