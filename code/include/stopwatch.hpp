template <typename clock>
class StopWatch
{
    struct Step
    {
        const std::string name;
        const clock::duration duration;
        const int memory_in_kb;
        const bool newline;
        Step(const std::string &name, const clock::duration &duration, const int &memory_in_kb, const bool &newline)
            : name(name), duration(duration), memory_in_kb(memory_in_kb), newline(newline)
        {
        }
        Step(const Step &step)
            : name(step.name), duration(step.duration), memory_in_kb(step.memory_in_kb), newline(step.newline)
        {
        }
    };

private:
    std::vector<StopWatch::Step> steps;
    bool started;
    bool paused;
    clock::time_point last_starting_time;
    std::string current_step_name;
    clock::duration stored_at_last_pause;
    bool newline;

    StopWatch()
    {
    }

    static int current_memory_use_in_kb()
    {
        std::ifstream procfile("/proc/self/status");
        std::string line;
        while (std::getline(procfile, line))
        {
            if (line.rfind("VmRSS:", 0) == 0)
            {
                // split in 3 pieces
                std::vector<std::string> parts;
                boost::split(parts, line, boost::is_any_of("\t "), boost::token_compress_on);
                // check that we have exactly thee parts
                if (parts.size() != 3)
                {
                    throw MyException("The line with VmRSS: did not split in 3 parts on whitespace");
                }
                if (parts[2] != "kB")
                {
                    throw MyException("The line with VmRSS: did not end in kB");
                }
                int size = std::stoi(parts[1]);
                procfile.close();
                return size;
            }
        }
        throw MyException("fail. could not find VmRSS");
    }

public:
    static StopWatch create_started()
    {
        StopWatch c = create_not_started();
        c.start_step("start");
        return c;
    }

    static StopWatch create_not_started()
    {
        StopWatch c;
        c.started = false;
        c.paused = false;
        c.newline = false;
        return c;
    }

    void pause()
    {
        if (!this->started)
        {
            throw MyException("Cannot pause not running StopWatch, start it first");
        }
        if (this->paused)
        {
            throw MyException("Cannot pause paused StopWatch, resume it first");
        }
        this->stored_at_last_pause += clock::now() - last_starting_time;
        this->paused = true;
    }

    void resume()
    {
        if (!this->started)
        {
            throw MyException("Cannot resume not running StopWatch, start it first");
        }
        if (!this->paused)
        {
            throw MyException("Cannot resume not paused StopWatch, pause it first");
        }
        this->last_starting_time = clock::now();
        this->paused = false;
    }

    void start_step(std::string name, bool newline = false)
    {
        if (this->started)
        {
            throw MyException("Cannot start on running StopWatch, stop it first");
        }
        if (this->paused)
        {
            throw MyException("This must never happen. Invariant is wrong. If stopped, there can be no pause active.");
        }
        this->last_starting_time = clock::now();
        this->current_step_name = name;
        this->started = true;
        this->newline = newline;
    }

    void stop_step()
    {
        if (!this->started)
        {
            throw MyException("Cannot stop not running StopWatch, start it first");
        }
        if (this->paused)
        {
            throw MyException("Cannot stop not paused StopWatch, unpause it first");
        }
        auto stop_time = clock::now();
        auto total_duration = (stop_time - this->last_starting_time) + this->stored_at_last_pause;
        // For measuring memory, we sleep 100ms to give the os time to reclaim memory
        // std::this_thread::sleep_for(std::chrono::milliseconds(100));
        int memory_use = current_memory_use_in_kb();

        this->steps.emplace_back(this->current_step_name, total_duration, memory_use, this->newline);
        this->started = false;
        this->newline = false;
    }

    std::string to_string()
    {
        if (this->started)
        {
            throw MyException("Cannot convert a running StopWatch to a string, stop it first");
        }
        std::stringstream out;
        for (auto step : this->get_times())
        {
            out << "Step: " << step.name << ", time = " << boost::chrono::ceil<boost::chrono::milliseconds>(step.duration).count() << " ms"
                << ", memory = " << step.memory_in_kb << " kB"
                << "\n";
            if (step.newline)
            {
                out << "\n";
            }
        }
        // Create the output string and remove the final superfluous '\n' characters
        std::string out_str = out.str();
        boost::trim(out_str);
        return out_str;
    }

    std::vector<StopWatch::Step>& get_times()
    {
        std::vector<StopWatch::Step> & steps_ref = this->steps;
        return steps_ref;
    }
};